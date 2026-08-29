# piggygang-indexer

General-purpose NFT indexer + REST API for the Piggy collections — 2 fixed
Metaplex Token Metadata collections (Piggy SOL Gang, Piggy Girl Gang) and 1
dynamic Metaplex Core collection (PiggyGang), ~15–25k NFTs. DAS backfill +
live streaming → attributes, owners, full tx history. Designed so any other
collection can be added later (registry-driven) and consume the same API.
Foundation for PiggyGang Explorer and future apps.

## Architecture decisions (ALG-618)

Locked 2026-08-27. Pricing below was verified against live provider pages on
that date — re-check before renewing/upgrading plans; Helius repriced twice in
2026 alone.

### Language: Rust

Team standard (piggygang-services is a Rust workspace with the same
actix-web + sqlx stack this repo mirrors). Helius ships an official,
actively-maintained Rust SDK for the future gRPC path (`helius-laserstream`,
tonic-based). The TypeScript SDK (Rust-core NAPI) was the defensible
alternative but diverges from the services standard and had open DX issues
(types collapsing to `any`, Debian glibc binary problems) at decision time.

### Ingest transport: Helius Enhanced WebSockets now, LaserStream gRPC later

**Helius plan: Developer ($49/mo).** The headline finding: mainnet LaserStream
gRPC no longer requires the $999 Professional plan — since 2026-04-07 it is
included in **Business ($499/mo)**. That is still disproportionate for this
scale, so we start without LaserStream:

- **Primary mainnet transport: Enhanced WebSockets** (`transactionSubscribe`,
  `accountSubscribe`/`programSubscribe`). Included in Developer. Since
  2026-03 these run on LaserStream infrastructure: ~24h replay, ordered
  delivery, multi-region. Metered at 20 credits/MB (~$100/TB) from the plan's
  10M monthly credits — tight filters are essential.
- **Supplementary:** Helius webhooks (1 credit/event, up to 100k addresses,
  but 3-retries-then-lost delivery — never a source of truth) and DAS
  reconciliation sweeps (10 credits/call; a full ~25k-asset re-scan is ~250
  credits, so a 15-min cadence is ~720k credits/mo).
- **Devnet LaserStream gRPC is included in Developer** — the gRPC adapter can
  be developed and tested against devnet without a plan upgrade.

**Upgrade/fallback matrix** (all behind the same `crates/ingest` interface —
swapping transports is a config change plus an adapter, zero pipeline edits):

| Transport | Cost (2026-08-27) | Notes |
|---|---|---|
| Enhanced WebSockets (chosen) | in Developer $49/mo | 24h replay, ordered; JSON adapter |
| LaserStream gRPC | Business $499/mo | 10 concurrent conns; free trial via contact form; same Yellowstone wire protocol |
| Third-party Yellowstone gRPC | Triton PAYG $125 deposit + $0.08/GB; Chainstack ~$98/mo; Shyft $199/mo | wire-compatible URL+token swap; no Helius 24h replay |
| Webhooks + DAS sweep | ~free (fits even lower tiers) | weakest delivery guarantees; DAS sweep becomes source of truth |

**Replay caveats:** the ~24h replay window delivers finalized-only data beyond
~20 minutes, and a disconnect mid-replay can skip slots (laserstream-sdk
issue #115). Durable resume therefore never trusts transport replay: the
consumer persists `last_processed_slot` in Postgres (`ingest_state`) and DAS
reconciliation (ALG-624) is the safety net. Downtime beyond the replay window
triggers targeted re-backfill.

### Hosting + Postgres: Railway (EU West Metal, Amsterdam)

Not Vercel — the consumer is a persistent WebSocket/gRPC client, so it needs a
long-running host. Railway (Hobby, ~$15–25/mo all-in) was chosen over Fly.io
(~$7–12/mo) and a Hetzner VPS (~€6/mo) for the smoothest deploy DX
(push-to-deploy gated by CI and a healthcheck) at comparable cost. Amsterdam
matches Helius's recommended EU endpoints (ams/fra). Postgres is a Railway
service in the same project, reached over private networking (no egress fees);
its `DATABASE_URL` is injected by reference variable. Railway Postgres backups
are snapshots (daily/weekly/monthly), not PITR — acceptable because the entire
DB is rebuildable from chain via DAS backfill.

TLS policy: **rustls everywhere** (sqlx `runtime-tokio-rustls`, reqwest
`rustls-tls`, tungstenite `rustls-tls-webpki-roots`) — no libssl in the
runtime image, distroless-ready.

## Repository layout

```
config/
  collections.toml   the collections/tokens registry seed — the ONLY home of on-chain addresses
  seeds/*.mints.json closed mint lists for Token Metadata collections without a certified collection
crates/
  config/      indexer-config — env config, fail-fast validation
  data-model/  indexer-data-model — Postgres migrations, registry, seed, ingest cursors, facet queries
  ingest/      indexer-ingest — transport-agnostic ingest interface + mock
services/
  admin/       indexer-admin — migrate | seed | bench
  api/         indexer-api — REST API (today: /health, /ready)
```

Future member (the workspace globs already cover it): `services/ingester`
(ALG-623). `railway.api.json` deliberately does not watch `config/**` or
`services/admin/**` — neither is part of the API image.

### The ingest abstraction

Everything downstream is written against `IngestSource` / `IngestEvent`
(`crates/ingest`); which wire events arrive on is an adapter detail. Key
contracts:

- **Normalized events, transport-neutral spec.** `SubscriptionSpec` compiles
  to one Yellowstone `SubscribeRequest` (gRPC) or per-entry WS subscribe
  calls. Events carry base58 strings and decoded bytes — adapters pay
  conversion at the edge, nothing downstream branches on transport.
- **`RawPayload` quarantine.** Transaction events keep the transport-native
  payload for classification, but only the ingest crate's own (future) decode
  module may look inside it. Pipeline code matching on `RawPayload` variants
  is a review error.
- **Consumer owns the durable cursor.** Adapters own reconnection; the
  consumer persists `last_processed_slot` only on `SlotCheckpoint` events and
  resumes with inclusive `ResumeFrom::Slot` → at-least-once delivery +
  signature-keyed idempotent upserts.
- **Live subscription updates** via a `watch::Receiver<SubscriptionSpec>` —
  adding a collection to the registry updates subscriptions without restart.
- **Errors are terminal.** `Ok` = data, `Status` = telemetry, `Err` = dead
  stream; restart policy belongs to the service, not the adapter.

`crates/ingest/tests/consumer_loop.rs` proves the crash/resume/redelivery
semantics against the mock through the public trait.

## Getting started

Prereqs: Rust via rustup (the pinned 1.90 toolchain auto-installs), Docker.

```sh
docker compose up -d                        # Postgres 17.6 on localhost:5433
cp .env.example .env                        # DATABASE_URL points at it; fill HELIUS_API_KEY when needed
cargo run -p indexer-admin -- migrate       # apply migrations (the API also does this at boot)
cargo run -p indexer-admin -- seed          # registry from config/collections.toml (idempotent)
cargo run -p indexer-api
curl localhost:8080/health                  # liveness, no DB
curl localhost:8080/ready                   # readiness: DB ping → 200 / 503
```

Checks (same as CI; the DB tests need `DATABASE_URL`, and are `#[ignore]`d
so a plain `cargo test --workspace` stays green without Postgres):

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace -- --include-ignored
cargo run -p indexer-admin -- seed --expect-unchanged   # the seed is a no-op the second time
```

## Configuration

| Var | Required | Default | Notes |
|---|---|---|---|
| `PORT` | no | `8080` | injected by Railway |
| `HOST` | no | `::` | dual-stack bind; Railway private networking needs the IPv6 bind |
| `RUST_LOG` | no | `info` | |
| `HELIUS_API_KEY` | not yet | — | required by ingest/backfill work (ALG-621/623) |
| `DATABASE_URL` | api, admin | — | on Railway set to `${{Postgres.DATABASE_URL}}`; the seed runs from a workstation against `DATABASE_PUBLIC_URL` |
| `DATABASE_MAX_CONNECTIONS` | no | `5` | pool size per process (Railway Postgres is shared by api, ingester, admin) |
| `DATABASE_CONNECT_TIMEOUT_SECS` | no | `5` | per-connection acquire timeout; boot retries connectivity for up to 60 s |

## Endpoints

- `GET /health` — liveness (no dependency checks; the Railway deploy gate):
  `{"status":"ok","service":"indexer-api","version":"0.1.0","commit":"<git sha>"}`
- `GET /ready` — readiness: `SELECT 1` with a 2 s bound → `200 {"status":"ready"}`
  or `503 {"status":"unavailable","reason":…}`. Outside `/v1` and outside the API contract.

## Collections registry (ALG-619)

The registry (`collections`, `collection_mints`, `tokens`) is data, seeded
from `config/collections.toml` + `config/seeds/*.mints.json` by
`indexer-admin seed` — upsert by slug, mint lists insert-only, nothing is
ever deleted, re-running is a no-op. **No on-chain address exists in Rust,
SQL or tests.** Registered today (all enabled): Piggy SOL Gang (10,000
mints), Piggy Girl Gang (5,000), Pig Mud (2,073) — all Metaplex Token
Metadata — Piggy Gang (Metaplex Core, dynamic), and the `$PIGGY` fungible
token (registry only, no balances).

Membership is derived by Postgres from the row (`membership_rule`); backfill,
live pipeline and reconciliation `match` on it, so a new collection is one
TOML entry, `seed`, backfill — zero code:

| Kind | TOML | rule | how members are found |
|---|---|---|---|
| Token Metadata with a certified collection | `standard="token_metadata"`, `address` | `tm_collection` | DAS `searchAssets` by collection |
| Token Metadata without one (the three Piggy TM collections: `collection: null` on chain) | `verified_creator`, `symbol`, `mints = { file, count }` | `tm_allowlist` | the committed mint list (`getAssetBatch`); creator/symbol are validation signals |
| Metaplex Core | `standard="core"`, `address` (CollectionV1) | `core_collection` | DAS `searchAssets` by collection; new mints appear automatically |
| Announced / unknown | `enabled = false`, nothing else | NULL | skipped |

Optional per-collection keys: `update_authority`, `image_url`,
`metadata_uri_template` (`{mint}` placeholder — used instead of the on-chain
URI when the original host is dead, as it is for Piggy Girl Gang),
`facet_exclude` (trait types stored but never faceted, e.g. Girl Gang's
per-asset-unique `Name`). The seed refuses to change a collection's
`standard`/`address`/`verified_creator` once it has assets unless
`--allow-identity-change` is given, and refuses a mint already owned by
another collection.

## Data model

Five forward-only migrations in `crates/data-model/migrations/` (never edit
one after it has been applied anywhere; expand-only across a release because
Railway overlaps old and new replicas and rollback re-deploys an older
binary). Tables:

- `assets` — one row per NFT (`address` = mint or Core asset id, `bigint`
  surrogate key), name → generated `number`, `burned`, `membership_status`,
  observed `owner` + `owner_slot`, `last_activity_*` (trigger-maintained),
  `image_status`. Fetched off-chain JSON lives in `asset_documents`.
- `trait_types` / `trait_values` / `asset_attributes` — per-collection
  dictionary + narrow `(asset, type, value)` rows; `facet_counts` view for the
  unfiltered counts, `indexer_data_model::facets` for the disjunctive
  (marketplace-style) filtered counts.
- `activity` — events keyed `(asset_id, signature, seq)` for at-least-once
  ingest; kinds `mint|transfer|sale|burn|stake|unstake|other` (the API serves
  the first four); shape CHECKs mirror the contract's nullability.
  `asset_signatures` keeps the raw per-asset crawl so ALG-622 can reclassify
  without refetching.
- `ownership_history` — intervals with a GiST `EXCLUDE` (no overlaps ⇒ at
  most one open interval per asset), deferrable; `integrity_*` views are
  what reconciliation (ALG-624) diffs.
- `ingest_state` (slot cursor per live stream, monotonic) and
  `backfill_state` (per-collection job cursors, opaque JSON).
- `collection_stats` view — supply, holders, 24h/7d activity.

The browse/facet population is "member assets of the collection, burned
included" (the UI greys burned NFTs); only `supply`/`holders` exclude burned.

### Facet performance (acceptance: < 100 ms)

`cargo run --release -p indexer-admin -- bench` seeds three synthetic
collections (10k PSG-like, 5k PGG-like with a unique-per-asset trait, 10k
Core-like — Piggy-scale, real cardinalities and skew) and times the
disjunctive facet query through the same sqlx path the API will use (the
statement is planned with real parameter values every time —
`persistent(false)` — so no generic-plan surprise). Measured locally on
29 Aug 2026 (PG 17.6 in Docker, M-series laptop): every scenario p50 ≤ 40 ms
(two active types on 10k assets: 39 ms; three types: 33 ms; text search:
5 ms), 5k-asset collection ≤ 22 ms; all three collections are timed. Re-run on real data after the backfill
with `bench --slug piggy-sol-gang` (the scenarios are derived from the
collection's own facet distribution); `--dirty` rewrites 20 % of the rows
first to mimic a live ingester. The query scans the collection's attribute
rows once, so a future 100k-asset external collection would land around
300 ms — the documented escape hatches are an API-side cache for the
unfiltered counts and a materialized `facet_counts`.

## Deployment

Railway, config-as-code in `railway.api.json` (builder, healthcheck, restart
policy, region). Flow: push to `main` → GitHub Actions CI → Railway (**Wait
for CI** enabled) builds the Dockerfile → `/health` must return 200 → traffic
cutover. Rollback: `git revert` + push, or dashboard → previous deployment →
Redeploy.

**Deployment contract:** the Dockerfile builds workspace binary
`${BIN}` (default `indexer-api`) and runs it as `app`; the process
must listen on `[::]:$PORT`; `RAILWAY_GIT_COMMIT_SHA` is passed as a build arg
and baked into `/health` as `commit`. The future ingester service reuses the
same Dockerfile with service variable `BIN=indexer-ingester` and its
own `railway.ingester.json` (`restartPolicyType: ALWAYS`, no healthcheck).

Railway gotchas (verified 2026-08-27):

- The healthcheck **gates deploys only** — it is not uptime monitoring. Add an
  external pinger for real alerting (ALG-628).
- **Wait for CI waits on ALL GitHub check suites** — a stray failing
  third-party check silently blocks deploys.
- The per-service **config file path** is set in the Railway dashboard and is
  absolute from repo root.
- Service variables reach the Docker build **only when the Dockerfile declares
  a matching `ARG`**.

### Deploy troubleshooting

The API boots by validating `DATABASE_URL`, logging the sanitized target
(`database target: <host>:<port>/<db> as <user>`), retrying connectivity for
up to 60 s and then running migrations; a failing migration is fatal on
purpose. Reading the log:

- Exits immediately with *unresolved Railway reference* or *points at
  localhost inside a Railway container* → the service variable is wrong. It
  must be exactly `${{Postgres.DATABASE_URL}}` — not the `.env.example` value
  with the reference appended, which is what produced
  `postgres://…@localhost:5433/piggygang_indexer${{…}}` on the first deploy.
- `database not reachable (Connection refused …)` against
  `postgres.railway.internal` → Postgres is down, or in another environment
  than the `api` service.
- `failed to lookup address` → private networking / DNS not ready; the retry
  covers Railway's start-up delay.
- Keep `RUST_LOG=info` in production: `debug` logs every SQL statement,
  including the full migration bodies.

### One-time bootstrap

1. Install the Railway GitHub App on the `piggygang` org (access to
   `piggygang/indexer`).
2. `railway login` → `railway init` (project `piggygang-indexer`).
3. In the dashboard set the region to **EU West Metal (Amsterdam)** *before*
   provisioning Postgres — volumes are region-pinned.
4. `railway add --database postgres` (confirm it landed in Amsterdam).
5. `railway add --repo piggygang/indexer`; in the dashboard: rename the
   service `api`, set Config file path `railway.api.json`, branch `main`,
   enable **Wait for CI**, confirm region.
6. Variables on `api`: `HELIUS_API_KEY`, `DATABASE_URL=${{Postgres.DATABASE_URL}}`,
   `RUST_LOG=info`.
7. Push → CI green → build → healthcheck → ACTIVE. `railway domain --service
   api`, then `curl https://<domain>/health`.
8. `railway link` in the repo dir (enables `railway logs` / `railway ssh`).

## Roadmap

- ALG-619 — data model & collections registry (migrations, `ingest_state`) — done
- ALG-620 — freeze v1 API contract (OpenAPI) + mock server for Explorer
- ALG-621 — DAS backfill (assets, attributes, owners)
- ALG-622 — historical activity backfill (archival API)
- ALG-623 — live pipeline: `ws` adapter (Enhanced WebSockets), ingester service
- ALG-624 — reconciliation: periodic DAS diff + self-heal
- ALG-625/626 — public REST API (browse/facets, detail/activity/portfolio)
- ALG-627 — rarity scoring · ALG-628 — prod monitoring/alerting · ALG-629 — external collections
