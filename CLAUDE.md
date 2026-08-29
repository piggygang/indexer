# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Git rules

- NEVER commit or push. When work is ready, suggest a commit message and let the user run git themselves.
- Suggested commit messages must be a single line, with no Co-Authored-By trailer or any other Claude/AI attribution.

## Commands

- `docker compose up -d` — local Postgres 17.6 on **localhost:5433** (5432 belongs to piggygang-services)
- `cargo run -p indexer-admin -- migrate` — apply migrations; `-- seed [--dry-run] [--expect-unchanged]` — apply `config/collections.toml`; `-- bench [--assets N] [--slug <slug>] [--dirty] [--clean]` — synthetic data + facet timings (the ALG-619 <100 ms evidence)
- `cargo run -p indexer-api` — API on `[::]:8080` (`PORT`/`HOST` env); needs `DATABASE_URL`, migrates at boot
- `cargo test --workspace` — DB tests are `#[ignore]`d (green without Postgres); `cargo test --workspace -- --include-ignored` runs them against `DATABASE_URL` (what CI does)
- `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --all --check` — CI gates (rustfmt defaults, no clippy config)

Toolchain is pinned by `rust-toolchain.toml` (1.90). Keep it in lockstep with the Dockerfile's `cargo-chef` tag and CI's toolchain when bumping.

## Architecture

Cargo workspace (`services/*` + `crates/*`), packages named `indexer-*`, all `publish = false`. Mirrors piggygang-services conventions (actix-web, sqlx). See README for the decision record (ALG-618): Rust + Helius Developer plan with Enhanced WebSockets ingest + Railway hosting.

- `crates/config` — env config. Missing var → default; present-but-unparseable → hard error (never `unwrap_or` a parse). `DATABASE_URL` is optional at parse time; binaries that need it call `required_url()` at boot.
- `crates/data-model` — Postgres schema (`migrations/`, embedded by `sqlx::migrate!()`), pool helpers, typed registry, seed loader, ingest cursors, facet queries, synthetic generator. Leaf crate: no dependency on `indexer-config` or `indexer-ingest`. Queries are runtime-checked `sqlx::query`/`query_as` (no `.sqlx` offline data, no DB at build time). Enums are `text + CHECK` in Postgres and `#[sqlx(type_name = "text")]` enums in Rust (`types.rs`).
- `crates/ingest` — the transport-agnostic ingest interface (`IngestSource`, `IngestEvent`, `SubscriptionSpec`) + `MockSource`. **Containment rule: pipeline code never matches on `RawPayload` variants** — only a future decode module inside this crate may. Real transports (`ws` = Enhanced WebSockets, `grpc` = LaserStream) arrive in ALG-623 as optional-dep features; don't add stub modules or empty features before then.
- `services/api` — actix-web binary. Routes register via `handlers::configure` so tests build the identical route table. `/health` is liveness-only by design (no DB ping — the Railway deploy gate must not depend on Postgres); `/ready` pings the DB. Boot validates `DATABASE_URL` (unresolved `${{…}}` references and `localhost` on Railway fail immediately), logs the sanitized target, retries connectivity with bounded backoff and runs migrations (advisory-locked in sqlx); a failing migration is fatal on purpose.
- `services/admin` — `indexer-admin` CLI: `migrate | seed | bench`. The only place that writes the registry.

### Data model rules

- **On-chain addresses live only in `config/collections.toml` and `config/seeds/*.mints.json`** — never in Rust, SQL or tests (tests use synthetic keys). Adding a collection = one TOML entry (+ mint list for a Token Metadata collection without a certified collection) → `seed` → backfill. No code.
- Membership is derived by Postgres from the registry row (`collections.membership_rule`: `core_collection` | `tm_collection` | `tm_allowlist`); pipeline code `match`es on `types::MembershipRule` — one arm per rule, never on slugs.
- The browse/facet population is `collection_id = $1 AND membership_status = 'member'` — burned assets included (the UI greys them); only `supply`/`holders` exclude burned. Every query over the population applies exactly that predicate.
- Migrations are forward-only and never edited once merged (sqlx checksums). Expand-only across a release: Railway keeps the old replica serving while the new one migrates, and the README rollback re-deploys an older binary against the newer schema.
- Ownership/activity writers (ALG-622/623) follow the contract in `migrations/20260829000400_activity_ownership.sql`: lock the asset, insert activity with `ON CONFLICT DO NOTHING … RETURNING`, mutate owner/history only when a row came back, flag `ownership_dirty` instead of applying out-of-order events. `ingest_state` is written only on `SlotCheckpoint`; `reset()` only with the ingester stopped.
- `openapi/v1.yaml` (ALG-620) is the frozen contract; the schema serves it, never changes it.

Ingest semantics (documented on the trait — keep them true): adapter owns reconnect; `Err` stream item is terminal; consumer persists `last_processed_slot` only on `SlotCheckpoint`; `ResumeFrom::Slot` is inclusive → at-least-once + signature-keyed idempotent upserts.

## Deployment

Railway (EU West Amsterdam), config-as-code in `railway.api.json`, deploys on push to `main` gated by CI (Wait for CI) and the `/health` healthcheck. One root `Dockerfile` builds any workspace binary via `ARG BIN` (default `indexer-api`); services must listen on `[::]:$PORT`. Secrets are env-only (`HELIUS_API_KEY`, `DATABASE_URL`) — never hardcode keys in source. `config/` is not in the image: run `seed` from a workstation against Railway's public database URL.
