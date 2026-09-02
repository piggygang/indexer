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
openapi/
  v1.yaml            the frozen v1 API contract (ALG-620); redocly.yaml lints it
crates/
  config/      indexer-config — env config, fail-fast validation
  data-model/  indexer-data-model — Postgres migrations, registry, seed, ingest cursors, facet queries
  ingest/      indexer-ingest — transport-agnostic ingest interface + mock
services/
  admin/       indexer-admin — migrate | seed | bench
  api/         indexer-api — REST API (today: /health, /ready)
```

Future member (the workspace globs already cover it): `services/ingester`
(ALG-623). The `api` service deliberately does not watch `config/**` or
`services/admin/**` — neither is part of the API image. The `admin` service
watches both, because the registry seed ships inside *its* image.

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
cargo run -p indexer-admin -- backfill      # DAS backfill (needs HELIUS_API_KEY; idempotent)
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
| `HELIUS_API_KEY` | `admin backfill` | — | Helius Developer key. Read only by the subcommand that needs it, so `migrate`/`seed` still run without one; ALG-623 will need it too |
| `DATABASE_URL` | api, admin | — | on Railway set to `${{Postgres.DATABASE_URL}}` (private network). The seed runs on the `admin` service; `DATABASE_PUBLIC_URL` is only for a workstation run |
| `DATABASE_MAX_CONNECTIONS` | no | `5` | pool size per process (Railway Postgres is shared by api, ingester, admin) |
| `DATABASE_CONNECT_TIMEOUT_SECS` | no | `5` | per-connection acquire timeout; boot retries connectivity for up to 60 s |

## Endpoints

- `GET /health` — liveness (no dependency checks; the Railway deploy gate):
  `{"status":"ok","service":"indexer-api","version":"0.1.0","commit":"<git sha>"}`
- `GET /ready` — readiness: `SELECT 1` with a 2 s bound → `200 {"status":"ready"}`
  or `503 {"status":"unavailable","reason":…}`. Outside `/v1` and outside the API contract.

## API contract (ALG-620)

`openapi/v1.yaml` is the **frozen** v1 contract — OpenAPI 3.1, hand-written.
ALG-625/626 implement it; the Explorer (ALG-630) generates its typed client from
it and develops against the mock. The schema serves the contract, never the
reverse. `/health` and `/ready` are deliberately outside it.

- `GET /v1/collections` · `GET /v1/collections/{slug}` — the registry plus
  `stats` (supply, holders, burned, indexed, 24h/7d activity, holder cohorts).
- `GET /v1/collections/{slug}/nfts` — the browse grid: `trait[Type]=Value`
  filters, `q`, `sort`, keyset `cursor`.
- `GET /v1/collections/{slug}/facets` — disjunctive trait counts plus `total`,
  the size of the filtered result set.
- `GET /v1/collections/{slug}/holders` · `/activity` — top holders; recent
  events, newest first, each carrying its NFT card.
- `GET /v1/nfts/{id}` · `/activity` · `/owners` — detail, timeline, ownership
  history. `{id}` is the base58 mint or Core asset address; internal ids are
  never exposed.
- `GET /v1/wallets/{address}/nfts` — portfolio grouped by collection. An unknown
  wallet is `200` with an empty portfolio, never `404`.
- `GET /v1/search` — smart search: a pasted mint or wallet resolves to a `route`;
  text and `#N` return hits grouped by collection.

Conventions: JSON fields are **camelCase**, enum *values* are the Postgres CHECK
literals (`token_metadata`, `mint`, `dead`); every property is present, with
explicit `null` rather than omission; keyset cursors are opaque and there is no
`total` on a page (the filtered count is `facets.total`); `priceLamports` is a
decimal **string** and `slot` a number; `X-RateLimit-*` and weak `ETag`s
throughout. Rarity fields exist but are `null` until ALG-627 — freezing them now
means that issue does not have to break a frozen contract.

### Mock server

```sh
docker compose up -d mock                        # Prism on :4010
curl -s localhost:4010/v1/collections | jq
curl -s -H 'Prefer: example=empty' localhost:4010/v1/collections/piggy-sol-gang/nfts
curl -s -H 'Prefer: code=429' localhost:4010/v1/collections
```

Static mode, so responses are the hand-written examples: real slugs, names,
symbols and supplies, but **synthetic** addresses (`SYN…`/`HLD…`/`Sgn…`) — real
on-chain addresses live only in `config/`. The browse example deliberately
includes a burned pig and an unnumbered one so the Explorer can build the greyed
and fallback card states. Named examples (`empty`, `lastPage`, `emptyWallet`,
`byMint`, `byWallet`, `nothing`) and `Prefer: code=<status>` cover the rest.

### One client-side caveat

OpenAPI leaves `deepObject` with array values undefined, and **openapi-fetch's
default query serializer throws** on the `trait` parameter. Supply one:

```ts
createClient<paths>({
  baseUrl: process.env.NEXT_PUBLIC_INDEXER_URL,   // mock: http://localhost:4010
  querySerializer(q) {
    const parts: string[] = [];
    for (const [k, v] of Object.entries(q ?? {})) {
      if (v == null) continue;
      if (k === "trait") {
        for (const [type, values] of Object.entries(v as Record<string, string[]>))
          for (const value of values)
            parts.push(`trait[${encodeURIComponent(type)}]=${encodeURIComponent(value)}`);
      } else if (Array.isArray(v)) {
        for (const item of v) parts.push(`${k}=${encodeURIComponent(String(item))}`);
      } else {
        parts.push(`${k}=${encodeURIComponent(String(v))}`);
      }
    }
    return parts.join("&");
  },
});
```

Prism ignores `trait` entirely, so the mock returns the same page with or
without it — filter *wiring* is only testable against the real API.

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

Railway, project config in `.railway/railway.ts` (Infrastructure-as-Code).
Flow: push to `main` → GitHub Actions CI → Railway (**Wait for CI** enabled)
builds the Dockerfile → `/health` must return 200 → traffic cutover. Rollback:
`git revert` + push, or dashboard → previous deployment → Redeploy.

### Services

| Service | Binary | Start | Healthcheck | Why it exists |
|---|---|---|---|---|
| `api` | `indexer-api` (default `BIN`) | image `CMD` | `/health`, 120 s | serves traffic |
| `admin` | `indexer-admin` (`BIN=indexer-admin`) | idles — see below | none | a container to `railway ssh` into and run `migrate` / `seed` against Postgres over the private network |

**Deployment contract:** the Dockerfile builds workspace binary
`${BIN}` (default `indexer-api`) and installs it under **its own name** at
`/usr/local/bin/${BIN}`, running as the unprivileged `app` user. The image
records the choice in `APP_BIN` and its `CMD` is
`/bin/sh -c 'exec "$APP_BIN"'`, because an exec-form `CMD` cannot expand a
build arg; `exec` hands PID 1 to the binary. There is deliberately **no
`ENTRYPOINT`** — a Railway start command replaces the ENTRYPOINT, and whether
an image `CMD` is then appended as arguments is undocumented. A service that
serves traffic must listen on `[::]:$PORT`; `RAILWAY_GIT_COMMIT_SHA` is passed
as a build arg and baked into `/health` as `commit`. The future ingester service
reuses the same Dockerfile with service variable `BIN=indexer-ingester` and
becomes a third `service()` in `.railway/railway.ts` (no healthcheck, restart
policy `ALWAYS`).

The `admin` container idles on purpose. `indexer-admin` is a CLI with a
*required* subcommand, so the image's default `CMD` would exit 2 immediately and
leave nothing to attach to. Its start command is

```sh
/bin/sh -c "trap 'exit 0' TERM INT; sleep infinity & wait"
```

Railway runs a start command **in exec form** — it replaces the image's
ENTRYPOINT and gets no shell, so `trap`, `&` and `$VAR` only work inside an
explicit `/bin/sh -c` wrapper. The trap matters because a bare `sleep infinity`
becomes PID 1, has no signal handler, and per `pid_namespaces(7)` ignores
SIGTERM, so every redeploy would wait out the grace period and SIGKILL. If the
quoting ever misbehaves, `/bin/sh -c "exec sleep infinity"` is the simpler
fallback (at the cost of that slower shutdown). `admin` has **no healthcheck** and **no
domain**: with `healthcheckPath` unset a deployment goes Active as soon as the
container starts, which is exactly what a service that never binds `$PORT`
needs. It costs roughly $0.15–$0.25/month idle.

`config/` ships in the **admin image only** — the builder stages it behind a
`${BIN}` check and the runtime overlays `/app/dist` onto `/app`, so the api
image stays config-free. CI asserts the `${BIN}` literal still matches
`services/admin/Cargo.toml`, because a package rename would otherwise produce a
green build whose `seed` fails only at run time.

### Infrastructure-as-Code

Railway **deprecated config-as-code**: new services cannot opt into
`railway.json`/`railway.toml`, and existing files stop being read on
**2026-12-01**. Project config therefore lives in `.railway/railway.ts`.

Prerequisites: **Node ≥ 22** (the CLI evaluates the file with
`node --experimental-strip-types`, added in 22.6) and the SDK installed at the
repo root — `npm install railway` — because the CLI resolves `railway/iac` from
`node_modules` rather than injecting it. `package.json`/`package-lock.json` are
committed for that reason; the Rust workspace has no Node runtime dependency.

```sh
railway config pull                          # live project → authoring code
railway config plan --detailed-exit-code     # read-only; 0 = no drift, 2 = pending
railway config apply --plan <pinned> --yes
```

**Always author `railway.ts` from `railway config pull`, never by hand and
never from `railway config migrate`.** IaC treats omission as deletion, so a
file that does not mention the Postgres service invites the reconciler to delete
Postgres *and its volume*; `pull` imports the whole project, secrets included as
`preserve()`. Before any `apply`, read the plan and abort on any
`delete`/`replace`/`recreate`, on any change under `Postgres`, or on any removal
of `HELIUS_API_KEY`. **Never pass `--confirm-destructive`** — needing it is the
abort signal, and usually means a service name's casing is wrong
(`Postgres`, not `postgres`).

Secrets stay in Railway: render them as `preserve()`, never as literals in
`railway.ts`. `.railway/` and `node_modules/` are dockerignored so the IaC
toolchain never enters the build context (both `COPY . .` stages would
otherwise bust the cargo-chef layer on every edit).

### The IaC option surface

`docs.railway.com/infrastructure-as-code/reference` documents only a subset —
it omits `build.builder`, `build.dockerfilePath`, `build.watchPatterns`, the
whole `deploy` block (restart policy, cron, `sleepApplication`, limit
overrides), `networking`, and `github(..., { checkSuites })`. **The
authoritative surface is the bundled type declarations in the `railway` npm
package** (`IntentServiceConfig` in `node_modules/railway/dist/`). Read those,
not the website, before concluding something must be set by hand — there are
currently **no dashboard-only settings** for this project.

Two more places the docs mislead:

- **Regions.** The IaC reference's example key is `europe-west4`, which is not a
  valid region id. `railway config pull` round-trips the short code — this
  project uses `ams`. The SDK does not validate region strings (`BucketRegion`
  is the only region union), so a wrong one type-checks and fails at apply.
- **Wait for CI** is `source: github(repo, { checkSuites: true })`.

Watch patterns matter more than they look: without them **every push rebuilds
every service**, including README-only commits, and an `admin` rebuild tears
down an SSH session mid-command. Railway's build cache is per service, so
`admin`'s first build recompiles the whole dependency graph.

### Operating the admin service

```sh
railway ssh keys add --key ~/.ssh/id_ed25519.pub --name "laptop"   # once per account
railway ssh --service admin -- indexer-admin migrate
railway ssh --service admin -- indexer-admin seed --config /app/config/collections.toml --dry-run
railway ssh --service admin -- indexer-admin seed --config /app/config/collections.toml
railway ssh --service admin -- indexer-admin seed --config /app/config/collections.toml --expect-unchanged
```

Pass `--config` as an **absolute path** so nothing depends on where the SSH
session lands, and prefer this non-interactive form: `railway ssh --session`
auto-installs tmux, which will not work here (the container runs as uid 10001
and apt lists are removed), and without tmux a dropped connection SIGHUPs the
command. That is safe — `seed` is a single transaction and idempotent, so an
interrupted run rolls back cleanly — but you would have to re-run it blind.

**Never run bare `indexer-admin bench` against production:** with no `--slug`
it seeds three synthetic `bench-*` collections into the live database, and
unlike `seed` it is not one transaction. Use
`indexer-admin bench --slug piggy-sol-gang`, and `indexer-admin bench --clean`
to remove bench data. Never `--dirty` on production.

Railway gotchas (verified 2026-08-30):

- The healthcheck **gates deploys only** — it is not uptime monitoring. Add an
  external pinger for real alerting (ALG-628).
- **Wait for CI waits on ALL GitHub check suites** — a stray failing
  third-party check silently blocks deploys.
- **Config-as-code is deprecated.** New services cannot use it; existing
  `railway.json`/`railway.toml` stop being read on **2026-12-01**. Use
  `.railway/railway.ts`, and see the IaC warnings above before any `apply`.
- **IaC omission means deletion** — a resource missing from `railway.ts` is a
  deletion candidate, Postgres and its volume included.
- **A config file's settings are never written back to the dashboard** — "the
  settings in the dashboard will not be updated with the settings defined in
  code". So a service migrating off `railway.json` must have those settings
  authored in `railway.ts` (or entered by hand) *before* the file is dropped.
- **The IaC docs are an incomplete subset** of the SDK's real option surface,
  and their region example is wrong. Trust the bundled `.d.ts`, not the website.
- **Node ≥ 22 is required** to evaluate `railway.ts`; on Node 20 the CLI fails
  with `node: bad option: --experimental-strip-types`.
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
   service `api`, branch `main`, enable **Wait for CI**, set Watch Paths,
   confirm region.
6. Variables on `api`: `HELIUS_API_KEY`, `DATABASE_URL=${{Postgres.DATABASE_URL}}`,
   `RUST_LOG=info`.
7. Push → CI green → build → healthcheck → ACTIVE. `railway domain --service
   api`, then `curl https://<domain>/health`.
8. `railway link` in the repo dir (enables `railway logs` / `railway ssh`).

### Adding the `admin` service

Done once, on top of the bootstrap above. Snapshot first — `railway variables
--service api` and `--service Postgres` to a directory **outside the repo**, and
confirm a recent Postgres backup exists.

1. `nvm use 24` (or newer) and `npm install railway`, then `railway config pull`
   to baseline the live project as code.
2. Add the `admin` service to `.railway/railway.ts` and author the settings the
   old `railway.api.json` supplied onto `api` — `build.builder`/`dockerfilePath`/
   `watchPatterns`, `healthcheck`, `healthcheckTimeout`, `deploy.restartPolicyType`
   and `restartPolicyMaxRetries`. Leave the Postgres and volume blocks
   byte-identical to what `pull` produced.
3. `railway config plan --out <file>`, read every line against the abort list in
   **Infrastructure-as-Code** above, then
   `railway config apply --plan <file> --yes`. Apply **before** touching the
   config-file setting: until this runs, the api's builder, watch patterns,
   healthcheck and restart policy are `null` in its stored settings.
4. Dashboard → `api` → Settings → **clear the Config file path**, then delete
   `railway.api.json` and push. A service managed by `railway.json` cannot also
   be managed by `railway.ts`.
5. Verify: `railway ssh --service admin -- indexer-admin --version`,
   `-- ls /app/config/seeds`, and that `DATABASE_URL` resolves to
   `postgres.railway.internal` (not `proxy.rlwy.net`, not a literal `${{…}}`).
   Then run `migrate` and `seed` per **Operating the admin service**.

#### Retiring `railway.api.json`

The repo no longer carries a config-as-code file; `.railway/railway.ts` is the
single source of truth. Railway **never writes config-file values back into the
dashboard**, so everything that file supplied had to be re-authored — in
`railway.ts`, since the SDK can express all of it — before the file was deleted.
Order, per service:

1. Author the settings in `railway.ts` and `railway config apply` them. Do this
   first: `railway config plan` against this project showed every one of those
   api settings as `null`, i.e. they lived only in the file and never reached
   the service's stored settings.
2. Clear the service's **Config file path** in the dashboard. A service managed
   by `railway.json` cannot also be managed by `railway.ts`, and what Railway
   does with a path pointing at a *missing* file is undocumented — so the path
   is cleared before the file is deleted, never after.
3. Delete the file and push.

## DAS backfill (ALG-621)

`indexer-admin backfill` fills `assets`, `asset_attributes` and
`asset_documents` from Helius DAS plus each collection's off-chain metadata.
It is the first thing that puts real chain state in the database, and
everything after it (ALG-622/623/624) builds on that baseline.

```sh
cargo run -p indexer-admin -- backfill --slug piggy-sol-gang --limit 25   # smoke run
cargo run -p indexer-admin -- backfill                                    # every enabled collection
cargo run -p indexer-admin -- backfill --check-images                     # opt-in reachability pass
cargo run -p indexer-admin -- backfill --expect-unchanged                 # proves a re-run changes nothing
```

**Membership comes from the registry, not from code.** The pass `match`es on
`collections.membership_rule`: `tm_allowlist` enumerates the committed mint
list and asks DAS `getAssetBatch` (1 000 ids per call, so the three Piggy
Token Metadata collections are ~18 calls); `core_collection` and
`tm_collection` page `searchAssets` by collection address. Adding a collection
stays one TOML entry plus `seed`.

**Idempotency is structural, not a convention.** Every upsert carries a
`WHERE … IS DISTINCT FROM …` guard, so an unchanged row produces no tuple and
never fires the `updated_at` trigger; `--expect-unchanged` exits non-zero if
anything moved, and `SELECT count(*) FROM assets WHERE updated_at > $t` is the
independent check. A second pass over an unchanged collection also issues zero
HTTP requests: a document is re-fetched only when it is missing, or when the
URI we would fetch today differs from the one recorded in
`metadata_source_uri`.

That last clause is what makes a dead metadata host recoverable with no code
change. Pig Mud's on-chain URIs point at the defunct shdw-drive host, so its
assets, names and owners come from DAS while its attributes stay empty and the
run reports the gap. Adding a `metadata_uri_template` to `config/collections.toml`
once the files are re-hosted changes the computed URI, and the next run
re-fetches and fills in. An asset whose document could not be read is
deliberately left out of the attribute delete scope, so a failed fetch never
wipes good data.

**What it does not write.** No `ownership_history`: DAS reports the current
owner but not the slot at which that ownership began, and stamping the
snapshot slot would make `/nfts/{id}/owners` claim a pig held since 2021 was
acquired today. The contract already promises `heldSince: null` until the
activity backfill (ALG-622) runs, and leaving the table empty keeps ALG-624's
"empty means healthy" integrity view meaningful. `image_status` is likewise
untouched unless `--check-images` is passed — the contract defines `unknown`
as "not checked, load optimistically".

`owner_slot` is stamped from a `getSlot` taken **before** each DAS call, a
conservative lower bound on the observation, and the writer only advances
ownership under `EXCLUDED.owner_slot > assets.owner_slot`. The slot alone is
never a reason to write, or every pass would rewrite every row.

Progress and results are durable in `backfill_state` (`kind = 'das_assets'`),
so a run's outcome is readable after the fact without scrollback:

```sh
psql "$DATABASE_URL" -c "SELECT c.slug, s.status, s.progress \
  FROM backfill_state s JOIN collections c ON c.id = s.collection_id \
 WHERE s.kind = 'das_assets';"
```

Ids DAS does not know are counted and sampled into `progress.missing`, never
invented as rows to make a supply count reconcile.

### Running it on Railway

Not wired up yet, and it needs two changes **in this order**, because IaC
treats a `preserve()` for a variable that does not exist as a change to
reconcile:

1. Set `HELIUS_API_KEY` on the `admin` service (dashboard or
   `railway variables --service admin --set ...`). Today the key exists only
   on `api`; `preserve()` does not copy a value between services.
2. Add `HELIUS_API_KEY: preserve()` to the `admin` env block in
   `.railway/railway.ts`, then `railway config plan`, read every line, then
   `apply`. Never `--confirm-destructive`.

Also note `DATABASE_MAX_CONNECTIONS = "2"` on `admin`: enough for the backfill,
which uses one pooled connection at a time, but leaves no headroom if a second
one-off run overlaps.

```sh
railway ssh --service admin -- indexer-admin backfill --slug piggy-sol-gang
```

## Roadmap

- ALG-619 — data model & collections registry (migrations, `ingest_state`) — done
- ALG-620 — freeze v1 API contract (OpenAPI) + mock server for Explorer — done
- ALG-621 — DAS backfill (assets, attributes, owners) — done
- ALG-622 — historical activity backfill (archival API)
- ALG-623 — live pipeline: `ws` adapter (Enhanced WebSockets), ingester service
- ALG-624 — reconciliation: periodic DAS diff + self-heal
- ALG-625/626 — public REST API (browse/facets, detail/activity/portfolio)
- ALG-627 — rarity scoring · ALG-628 — prod monitoring/alerting · ALG-629 — external collections
