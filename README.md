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
crates/
  config/    indexer-config — env config, fail-fast validation
  ingest/    indexer-ingest — transport-agnostic ingest interface + mock
services/
  api/       indexer-api — REST API (today: /health hello service)
```

Future members (the workspace globs already cover them):
`crates/data-model` (ALG-619), `services/ingester` (ALG-623).

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
docker compose up -d           # Postgres 17.6 on localhost:5433 (not used yet — ALG-619)
cp .env.example .env           # then fill HELIUS_API_KEY when needed
cargo run -p indexer-api
curl localhost:8080/health
```

Checks (same as CI):

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Configuration

| Var | Required | Default | Notes |
|---|---|---|---|
| `PORT` | no | `8080` | injected by Railway |
| `HOST` | no | `::` | dual-stack bind; Railway private networking needs the IPv6 bind |
| `RUST_LOG` | no | `info` | |
| `HELIUS_API_KEY` | not yet | — | required by ingest/backfill work (ALG-621/623) |
| `DATABASE_URL` | not yet | — | becomes required in ALG-619; on Railway set to `${{Postgres.DATABASE_URL}}` |

## Endpoints

- `GET /health` — liveness (no dependency checks):
  `{"status":"ok","service":"indexer-api","version":"0.1.0","commit":"<git sha>"}`
- `GET /ready` — planned (ALG-619): DB ping.

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

- ALG-619 — data model & collections registry (migrations, `ingest_state`)
- ALG-620 — freeze v1 API contract (OpenAPI) + mock server for Explorer
- ALG-621 — DAS backfill (assets, attributes, owners)
- ALG-622 — historical activity backfill (archival API)
- ALG-623 — live pipeline: `ws` adapter (Enhanced WebSockets), ingester service
- ALG-624 — reconciliation: periodic DAS diff + self-heal
- ALG-625/626 — public REST API (browse/facets, detail/activity/portfolio)
- ALG-627 — rarity scoring · ALG-628 — prod monitoring/alerting · ALG-629 — external collections
