// Railway project config (Infrastructure-as-Code). Applied from a workstation,
// one environment at a time:
//   railway config plan  --environment <production|devnet>   → review every line
//   railway config apply --environment <production|devnet>
//
// This file replaces the deprecated railway.api.json. Railway closed config-as-
// code to new services and stops reading those files on 2026-12-01.
//
// Authored from `railway config pull`, never by hand from scratch: IaC treats
// omission as deletion, so a file that fails to mention Postgres or its volume
// invites the reconciler to delete them. `pull` writes one environment, flat,
// so with two it is a comparison only — `railway config pull --json
// --environment <env>` — and never `--force`d over this file. Requires Node >=
// 22 and the local `railway` SDK (`npm ci`) — the CLI resolves `railway/iac`
// from node_modules.
//
// NOTE: docs.railway.com/infrastructure-as-code/reference documents only a
// subset of the options used below. The authoritative surface is the bundled
// type declarations in the `railway` npm package (IntentServiceConfig).
import { defineRailway, github, postgres, preserve, project, service, volume } from "railway/iac";

const REPO = "piggygang/indexer";
/** EU West Metal, Amsterdam. Short code — what `config pull` round-trips. */
const REGION = "ams";
/** Build inputs shared by every service: a change here rebuilds all of them. */
const COMMON = ["crates/**", "Cargo.toml", "Cargo.lock", "Dockerfile"];

// One Railway environment per cluster (PIG-116, architecture §8): each has its
// own Postgres, variables and domains, and all of them run the same code from
// `main`. The CLI hands the program below the environment it is planning
// (`ctx.environment`, from `--environment` or the link), and the program renders
// that environment from this table. Everything outside the table is identical
// across environments.
//
// An environment the table does not name is an error, never a fallback: a
// default here would plan one cluster's endpoints into another's Postgres. So a
// new environment is an entry here first, then `railway environment new`, then
// a plan.
//
// Which chain an environment indexes, and the endpoints that reach it
// (PIG-113), are literals, not preserve(): none of them is a secret — the
// Helius key is appended by the binaries from HELIUS_API_KEY — and writing them
// here is the point. `SOLANA_CLUSTER` is REQUIRED by every binary and has no
// default, and no endpoint is ever derived from it, so each service declares
// exactly the ones it dials:
//
//   api       SOLANA_CLUSTER                      (dials nothing; /health reports it)
//   admin     + SOLANA_RPC_URL, HELIUS_WEBHOOK_API  (backfills; `webhook`)
//   ingester  + SOLANA_RPC_URL, SOLANA_WS_URL
//
// A required variable inverts the usual order: **apply before the merge that
// needs it**, in every environment, because a merge never applies this file and
// every environment deploys `main`. Setting a variable redeploys the binary that
// is already running, which ignores a name it does not know; the new binary then
// finds it on its first boot. The other way round, the new image fails its
// healthcheck and Railway keeps the old deployment — safe, but nothing ships
// until the variable exists.
//
// A hostname that names another cluster is refused at parse time, so a devnet
// value pasted into the production entry fails the deploy rather than indexing
// devnet into that Postgres.
interface Environment {
  /** `devnet` | `mainnet-beta` — the SOLANA_CLUSTER spelling, exactly. */
  cluster: string;
  /** JSON-RPC + DAS. Bare endpoint: no query string, no key. */
  rpcUrl: string;
  /** Enhanced WebSockets. Its own variable — never the RPC URL with the scheme swapped. */
  wsUrl: string;
  /** Webhook management API (`indexer-admin webhook`). */
  webhookApi: string;
  /**
   * Custom domains. IaC **cannot register one** — `config plan` refuses
   * outright ("Custom-domain registration is not supported by Railway
   * configuration") — so a domain is listed here only once Railway has it.
   * A new environment's first apply runs without them; then, per service:
   *
   *   railway domain <host> --port 8080 --service <api|ingester> --environment <env>
   *
   * Port 8080 is not optional: this form compiles to `{ port: 8080 }` in the
   * SDK, so a domain created against any other target port leaves `config
   * plan` with a change it can never converge on. Omitting a domain is NOT a
   * deletion — the networking sub-maps merge rather than replace — so leaving
   * one out costs nothing but documentation.
   */
  apiDomain?: string;
  ingesterDomain?: string;
  /** Where Helius delivers, i.e. the ingester's own domain. Registration only. */
  webhookUrl: string;
  /** INGEST_TRANSPORTS: `ws`, `webhook` or `ws,webhook`. */
  transports: string;
  /** Variables this environment's api carries beyond the shared set; see production. */
  apiKept?: Record<string, ReturnType<typeof preserve>>;
}

const ENVIRONMENTS: Record<string, Environment> = {
  production: {
    cluster: "mainnet-beta",
    rpcUrl: "https://mainnet.helius-rpc.com",
    wsUrl: "wss://mainnet.helius-rpc.com",
    webhookApi: "https://api-mainnet.helius-rpc.com", // what the binary used to default to
    apiDomain: "api.indexer.piggygang.net",
    ingesterDomain: "ingester.indexer.piggygang.net",
    webhookUrl: "https://ingester.indexer.piggygang.net/webhooks/helius",
    // The WebSocket lane is retired here, by the cutover the README describes
    // under "Deciding when to retire the WebSocket". It was made in the
    // dashboard; this line records it, so an apply no longer turns the socket
    // back on. The on-Connected reconcile followed it to the webhook lane.
    transports: "webhook",
    // Set on the api before this file managed it, and kept rather than owned,
    // because omission is deletion: a HELIUS_API_KEY the api never reads (it
    // dials nothing), a HOST equal to the default, and a RUST_LOG chosen in the
    // dashboard. A new environment starts without all three.
    apiKept: { HELIUS_API_KEY: preserve(), HOST: preserve(), RUST_LOG: preserve() },
  },
  devnet: {
    cluster: "devnet",
    rpcUrl: "https://devnet.helius-rpc.com",
    wsUrl: "wss://devnet.helius-rpc.com",
    webhookApi: "https://api-devnet.helius-rpc.com",
    apiDomain: "api.devnet.indexer.piggygang.net",
    ingesterDomain: "ingester.devnet.indexer.piggygang.net",
    webhookUrl: "https://ingester.devnet.indexer.piggygang.net/webhooks/helius",
    // Whatever production runs, so end-to-end tests exercise the path
    // production really uses.
    transports: "webhook",
  },
};

function environmentFor(name: string | undefined): Environment {
  if (name !== undefined && Object.hasOwn(ENVIRONMENTS, name)) return ENVIRONMENTS[name];
  const known = Object.keys(ENVIRONMENTS).join(", ");
  const given = name === undefined ? "no environment was given" : `\`${name}\` is not one of them`;
  throw new Error(
    `.railway/railway.ts describes the Railway environments ${known}, and ${given}. ` +
      "Add an entry to ENVIRONMENTS before planning a new one: there is no default, " +
      "because a default would plan another cluster's endpoints into it.",
  );
}

// Deliberately NOT set here: deploy.restartPolicyType / restartPolicyMaxRetries
// and deploy.sleepApplication. The SDK accepts them and `config plan` reports
// them as pending, but `config apply` silently drops them — a read-back with
// `config pull --json` shows they never reach the service. Their values would
// be Railway's defaults anyway (ON_FAILURE, 10 retries, app-sleeping off), so
// omitting them costs nothing and keeps `railway config plan` converging to
// "no changes" — which is what makes it usable as a drift check. Verified
// 2026-08-31 against CLI 5.45.10 / railway@3.11.0.

export default defineRailway((ctx) => {
  const target = environmentFor(ctx.environment);

  const Postgres = postgres("Postgres", { region: REGION });
  const postgresVolume = volume("postgres-volume", {
    alerts: { usage: { "100": {}, "80": {}, "95": {} } },
    allowOnlineResize: true,
    region: REGION,
    sizeMB: 50000,
  });

  // The REST API. `/health` gates the deploy (liveness only, no DB ping) so a
  // Postgres blip can never block a cutover.
  const api = service("api", {
    source: github(REPO, { branch: "main", checkSuites: true }), // checkSuites = Wait for CI
    build: {
      builder: "DOCKERFILE",
      dockerfilePath: "Dockerfile",
      watchPatterns: ["services/api/**", ...COMMON],
    },
    healthcheck: "/health",
    healthcheckTimeout: 120,
    replicas: { [REGION]: 1 },
    ...(target.apiDomain ? { domains: [target.apiDomain] } : {}),
    networking: { privateNetworkEndpoint: "indexer" },
    env: {
      DATABASE_URL: Postgres.env.DATABASE_URL, // private: postgres.railway.internal
      RUST_LOG: "info",
      SOLANA_CLUSTER: target.cluster, // required; the api dials no endpoint, so it has none
      ...target.apiKept,
    },
  });

  // Operational CLI (indexer-admin: migrate | seed | bench). It exists to be
  // `railway ssh`'d into and serves no traffic, so it has no healthcheck —
  // unset means the deploy goes Active as soon as the container starts, which
  // is what a service that never binds $PORT needs — and no domain.
  const admin = service("admin", {
    source: github(REPO, { branch: "main", checkSuites: true }),
    build: {
      builder: "DOCKERFILE",
      dockerfilePath: "Dockerfile",
      // config/** is watched here and not on api: the registry seed ships in
      // the admin image only.
      watchPatterns: ["services/admin/**", "config/**", ...COMMON],
    },
    // indexer-admin is a CLI with a required subcommand, so the image's default
    // CMD would exit 2 and leave nothing to attach to. Railway runs a start
    // command in EXEC form — it replaces the ENTRYPOINT and gets no shell — so
    // the /bin/sh wrapper is required for `trap`. Without the trap the idle
    // process is PID 1 with no handler and ignores SIGTERM per
    // pid_namespaces(7), making every redeploy wait out the grace period.
    start: `/bin/sh -c "trap 'exit 0' TERM INT; sleep infinity & wait"`,
    replicas: { [REGION]: 1 },
    env: {
      BIN: "indexer-admin", // reaches the build only because the Dockerfile declares ARG BIN
      DATABASE_URL: Postgres.env.DATABASE_URL, // private: postgres.railway.internal
      // Set live on `admin` for the DAS backfill. Declared here because IaC
      // treats omission as deletion: while this line was missing, every
      // `config plan` carried a destructive `- Delete variable
      // admin.HELIUS_API_KEY`, which made the drift check unusable.
      HELIUS_API_KEY: preserve(),
      RUST_LOG: "info",
      DATABASE_MAX_CONNECTIONS: "2", // interactive, one-off; leave headroom for api
      SOLANA_CLUSTER: target.cluster,
      // `backfill` and `backfill-activity` verify this endpoint's genesis hash
      // against the cluster before they write.
      SOLANA_RPC_URL: target.rpcUrl,
      // `webhook` only. It used to be a default inside the binary; declared so
      // `webhook --dry-run` keeps working from `railway ssh` as it did.
      HELIUS_WEBHOOK_API: target.webhookApi,
    },
  });

  // The live pipeline (ALG-623) and, since the webhook transport, an HTTP
  // endpoint: Helius posts deliveries to /webhooks/helius, which this service
  // files into `webhook_inbox` and drains itself. It therefore now binds $PORT
  // and takes a healthcheck and a domain.
  //
  // The healthcheck is a deliberate behaviour change: a broken ingester used to
  // go Active the moment its container started, and now fails its deploy
  // instead. `/health` is liveness-only (no DB ping) so a Postgres blip cannot
  // block a cutover — the same rule `api` follows.
  //
  // Restart policy: the README used to promise ALWAYS here, but
  // `restartPolicyType` is one of the three fields `config apply` silently
  // drops (see the note above), so the binary supervises itself instead — it
  // re-subscribes with capped backoff forever and reserves a non-zero exit for
  // config/database failures. A watchdog exits if the checkpoint stops
  // advancing, and Railway's default ON_FAILURE brings it back into a
  // reconciling restart.
  const ingester = service("ingester", {
    source: github(REPO, { branch: "main", checkSuites: true }),
    build: {
      builder: "DOCKERFILE",
      dockerfilePath: "Dockerfile",
      watchPatterns: ["services/ingester/**", ...COMMON],
    },
    healthcheck: "/health",
    healthcheckTimeout: 120,
    // One replica, still: two would double-subscribe the WebSocket. The webhook
    // half would survive it (the inbox claim is FOR UPDATE SKIP LOCKED), but
    // the socket half would not.
    replicas: { [REGION]: 1 },
    ...(target.ingesterDomain ? { domains: [target.ingesterDomain] } : {}),
    env: {
      BIN: "indexer-ingester", // reaches the build only because the Dockerfile declares ARG BIN
      DATABASE_URL: Postgres.env.DATABASE_URL, // private: postgres.railway.internal
      // Both transports write through the same signature-keyed idempotent
      // writer, so a dual run (`ws,webhook`) makes double delivery a no-op and
      // the WebSocket's loss rate measurable instead of assumed. Retirement is
      // this one value becoming "webhook"; the on-Connected reconcile follows
      // it automatically.
      INGEST_TRANSPORTS: target.transports,
      // The whole Authorization header Helius echoes back, and it must be set
      // in the dashboard **before** the apply that turns the webhook lane on,
      // not after: `serve()` calls `required_webhook_secret()?`, so the
      // ingester fails to boot without it — and now that /health gates the
      // deploy, that fails the deploy rather than degrading. preserve() cannot
      // cover the gap; it preserves nothing on a variable that does not exist
      // yet. Keep it identical to what `indexer-admin webhook` registers, or
      // every delivery is a 401. One per environment: each registers its own
      // webhook, with its own secret.
      HELIUS_WEBHOOK_SECRET: preserve(),
      WEBHOOK_URL: target.webhookUrl,
      // preserve() keeps the live secret without writing it to source — but it
      // preserves nothing on a service that does not exist yet, and does NOT
      // copy a value between services or environments. On the apply that first
      // creates this service, set the key the moment `apply` returns (see the
      // README) — the cold cargo-chef build is the window, and until the
      // variable exists `config plan` keeps reporting a pending change.
      HELIUS_API_KEY: preserve(),
      RUST_LOG: "info",
      // One for the live writer, one for the reconcile a consumer spawns on
      // `Connected`, one for the scheduler (tip probe or sweep — `run_job`
      // runs one at a time), one for a Core mint's background attribute
      // hydration, one for the webhook drain, one for the receiver's inbox
      // writer task, one spare. It was 3 while the on-connect reconcile was
      // awaited inline and could not overlap anything.
      //
      // The HTTP handler itself needs none: it hands its batch to the writer
      // task on the main runtime rather than touching the pool from an actix
      // worker, whose runtime can be dropped out from under a connection.
      DATABASE_MAX_CONNECTIONS: "7",
      SOLANA_CLUSTER: target.cluster,
      // The ingester asks this endpoint for its genesis hash before anything
      // ingests, and /health answers 503 until the answer is this cluster's. So
      // a wrong value here — another chain, or a host that never answers —
      // fails the deploy instead of running a sweep that marks a whole Core
      // collection `removed`.
      SOLANA_RPC_URL: target.rpcUrl,
      // Read only with the `ws` transport; declared regardless, so turning the
      // lane back on is the one INGEST_TRANSPORTS change.
      SOLANA_WS_URL: target.wsUrl,
    },
  });

  return project("Indexer", {
    resources: [api, admin, ingester, Postgres, postgresVolume],
  });
});
