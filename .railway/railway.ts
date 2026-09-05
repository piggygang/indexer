// Railway project config (Infrastructure-as-Code). Applied from a workstation:
//   railway config plan   → review every line
//   railway config apply
//
// This file replaces the deprecated railway.api.json. Railway closed config-as-
// code to new services and stops reading those files on 2026-12-01.
//
// Authored from `railway config pull`, never by hand from scratch: IaC treats
// omission as deletion, so a file that fails to mention Postgres or its volume
// invites the reconciler to delete them. Requires Node >= 22 and the local
// `railway` SDK (`npm install railway`) — the CLI resolves `railway/iac` from
// node_modules.
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

// Deliberately NOT set here: deploy.restartPolicyType / restartPolicyMaxRetries
// and deploy.sleepApplication. The SDK accepts them and `config plan` reports
// them as pending, but `config apply` silently drops them — a read-back with
// `config pull --json` shows they never reach the service. Their values would
// be Railway's defaults anyway (ON_FAILURE, 10 retries, app-sleeping off), so
// omitting them costs nothing and keeps `railway config plan` converging to
// "no changes" — which is what makes it usable as a drift check. Verified
// 2026-08-31 against CLI 5.45.10 / railway@3.11.0.

export default defineRailway(() => {
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
    domains: ["api.indexer.piggygang.net"],
    networking: { privateNetworkEndpoint: "indexer" },
    env: {
      DATABASE_URL: preserve(),
      HELIUS_API_KEY: preserve(),
      HOST: preserve(),
      RUST_LOG: preserve(),
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
    },
  });

  // The live pipeline (ALG-623). It binds no port, so it gets no healthcheck
  // and no domain — an unset healthcheck makes the deploy go Active as soon as
  // the container starts, which is what a service that serves no traffic needs.
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
    replicas: { [REGION]: 1 },
    env: {
      BIN: "indexer-ingester", // reaches the build only because the Dockerfile declares ARG BIN
      DATABASE_URL: Postgres.env.DATABASE_URL, // private: postgres.railway.internal
      // preserve() keeps the live secret without writing it to source — but it
      // preserves nothing on a service that does not exist yet, and does NOT
      // copy a value between services. On the apply that first creates this
      // service, set the key the moment `apply` returns (see the README) —
      // the cold cargo-chef build is the window, and until the variable
      // exists `config plan` keeps reporting a pending change.
      HELIUS_API_KEY: preserve(),
      RUST_LOG: "info",
      // One connection for the live writer, one for the concurrent reconciler,
      // one spare.
      DATABASE_MAX_CONNECTIONS: "3",
    },
  });

  return project("Indexer", {
    resources: [api, admin, ingester, Postgres, postgresVolume],
  });
});
