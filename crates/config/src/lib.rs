//! Environment-based configuration.
//!
//! A missing variable falls back to its default; a variable that is present
//! but unparseable is a hard error — a typo'd `PORT` must fail the boot, not
//! silently bind the default.

use std::env;
use std::fmt::Display;
use std::str::FromStr;

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub helius: HeliusConfig,
    pub database: DatabaseConfig,
    pub ingest: IngestConfig,
    pub reconcile: ReconcileConfig,
    pub rarity: RarityConfig,
}

/// Which transports the ingester runs, and how the webhook drain paces itself.
///
/// The WebSocket and the Helius webhook are both `IngestSource`s and both
/// write through the same signature-keyed idempotent writer, so running them
/// together is safe and is how the WebSocket's loss rate becomes measurable
/// instead of assumed. `INGEST_TRANSPORTS=webhook` alone is the retirement.
#[derive(Debug, Clone)]
pub struct IngestConfig {
    /// `INGEST_TRANSPORTS`, default `ws`. Comma-separated; an unknown member
    /// is a hard error rather than a silently ignored typo that would leave a
    /// transport unrun.
    pub transports: Vec<Transport>,
    /// `WEBHOOK_POLL_MS`, default 1000. Skipped entirely after a full batch,
    /// so a burst drains continuously and an idle inbox costs one indexed
    /// query a second.
    pub webhook_poll_ms: u64,
    /// `WEBHOOK_BATCH`, default 50. Each row is one `getTransaction`, so a
    /// batch is a few seconds of work; small batches bound both the crash
    /// replay window and the gap between checkpoint opportunities.
    pub webhook_batch: i64,
    /// `WEBHOOK_LEASE_SECS`, default 60. How long a claimed row stays claimed
    /// before another drain may take it — the crash-recovery window.
    pub webhook_lease_secs: u64,
    /// `WEBHOOK_MAX_ATTEMPTS`, default 5. A row `getTransaction` never returns
    /// would otherwise be re-claimed forever *and* pin the watermark.
    pub webhook_max_attempts: i16,
    /// `WEBHOOK_GRACE_SECS`, default 120. How long a processed row must settle
    /// before the watermark counts it — the margin against a delivery that has
    /// not arrived at all yet, which is the case `min(pending)` cannot see.
    pub webhook_grace_secs: u64,
    /// `WEBHOOK_RETAIN_DAYS`, default 30. Must outlast the dual-run evaluation
    /// window: `webhook_inbox::coverage` reads these rows, so pruning early
    /// deletes the evidence the retirement decision rests on.
    pub webhook_retain_days: u32,
}

/// One ingest transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    /// Helius Enhanced WebSockets. No replay: `ResumeFrom::Slot` is a floor.
    Ws,
    /// Helius webhooks, received by this service and drained from
    /// `webhook_inbox`.
    Webhook,
}

impl FromStr for Transport {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim() {
            "ws" => Ok(Self::Ws),
            "webhook" => Ok(Self::Webhook),
            other => anyhow::bail!("unknown transport '{other}' (ws | webhook)"),
        }
    }
}

impl IngestConfig {
    pub fn runs(&self, transport: Transport) -> bool {
        self.transports.contains(&transport)
    }

    /// The lane that owns the on-`Connected` DAS reconcile.
    ///
    /// The first enabled transport in a fixed precedence, never a second env
    /// var: retiring the WebSocket then moves the reconcile to the webhook
    /// lane on its own, with nothing left to forget. Two lanes reconciling
    /// would ask Helius for twice the rate budget `RECONCILE_RPS` allows.
    pub fn reconciler(&self) -> Option<Transport> {
        [Transport::Ws, Transport::Webhook]
            .into_iter()
            .find(|t| self.runs(*t))
    }
}

/// Cadence of the rarity drain (ALG-627).
///
/// A backstop, not the trigger: every writer that can stale a rank flags its
/// collection, and the drain runs whatever is flagged on the next tick. The
/// interval only bounds how long a flag can sit unnoticed if a flagging write
/// happened while no drain was running.
#[derive(Debug, Clone)]
pub struct RarityConfig {
    /// `RARITY_INTERVAL_SECS`, default 86400 (a day). Zero disables the drain,
    /// leaving `indexer-admin rarity` as the only way to recompute.
    pub interval_secs: u64,
}

impl RarityConfig {
    pub fn enabled(&self) -> bool {
        self.interval_secs > 0
    }
}

/// Cadence of the ingester's periodic reconciliation (ALG-624).
///
/// Env rather than CLI flags because the ingester has no CLI. Both intervals
/// are affordable: a full sweep is ~20 DAS calls and ~200 credits, so hourly
/// costs about 144k credits a month, and the deep pass re-fetches only the
/// documents whose URI changed.
#[derive(Debug, Clone)]
pub struct ReconcileConfig {
    /// `RECONCILE_INTERVAL_SECS`, default 3600. The state sweep plus targeted
    /// activity recovery. Zero disables the schedule — reconciliation then
    /// happens only on `Connected`, as it did before ALG-624.
    pub interval_secs: u64,
    /// `RECONCILE_DEEP_INTERVAL_SECS`, default 604800 (7 days). Supply,
    /// burned assets and attribute changes, through the DAS backfill.
    pub deep_interval_secs: u64,
    /// `RECONCILE_RPS`, default 10 — the DAS rate limit on the Helius
    /// Developer plan, which is a *separate* bucket from RPC's 50/s. The sweep
    /// shares a rate budget with the live path, so it is throttled where the
    /// live consumer is not.
    pub rps: u32,
    /// `RECONCILE_TIP_INTERVAL_SECS`, default 30. The tip probe: one
    /// `searchAssets` per collection filter, newest-acted-on first, which
    /// finds what moved without re-reading everything. Three calls and ~1.4 s
    /// against the full sweep's 23 calls and ~35 s, which is what makes this
    /// cadence affordable — roughly 2.6M credits a month, a quarter of the
    /// plan. Zero disables it, leaving the full sweep as the only tier.
    pub tip_interval_secs: u64,
}

impl ReconcileConfig {
    /// Is the periodic full sweep on at all?
    pub fn enabled(&self) -> bool {
        self.interval_secs > 0
    }

    /// Is the tip probe on? Independent of the sweep: the probe needs no
    /// cursor and no DAS enumeration, so disabling one must not disable the
    /// other.
    pub fn tip_enabled(&self) -> bool {
        self.tip_interval_secs > 0
    }
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Bind address. Default `::` serves both IPv6 and IPv4 — Railway's
    /// private networking requires the IPv6 bind.
    pub host: String,
    /// Railway injects `PORT`; locally defaults to 8080.
    pub port: u16,
}

#[derive(Debug, Clone)]
pub struct HeliusConfig {
    /// Not needed by the API service; the backfill (ALG-621) and the future
    /// ingester (ALG-623) call [`HeliusConfig::required_api_key`] in the
    /// subcommand that needs it, so a missing key never breaks `migrate` or
    /// `seed`.
    pub api_key: Option<String>,
    /// `HELIUS_WEBHOOK_SECRET`. The **whole** `Authorization` header value
    /// Helius echoes back on every delivery — it is set as the webhook's
    /// `authHeader` and compared verbatim, so there is no `Bearer ` to parse.
    /// Helius offers no HMAC or signature scheme; this is the entire
    /// authenticity check, so it wants real entropy.
    pub webhook_secret: Option<String>,
    /// `WEBHOOK_URL` — the public HTTPS endpoint Helius posts to, registered
    /// by `indexer-admin webhook`. Never read on the receiving path.
    pub webhook_url: Option<String>,
    /// `HELIUS_WEBHOOK_API`, default `https://api-mainnet.helius-rpc.com`.
    /// Configurable because Helius's docs and their own SDK disagree on the
    /// host: the docs say `mainnet.helius-rpc.com`, the SDK uses this one, and
    /// a legacy `api.helius.xyz` is still in circulation. The subcommand's
    /// first call is a free `GET`, so a wrong host fails cheaply and loudly.
    pub webhook_api: String,
}

impl HeliusConfig {
    /// The key, or a hard error naming the variable — same idiom as
    /// [`DatabaseConfig::required_url`].
    pub fn required_api_key(&self) -> Result<&str> {
        self.api_key
            .as_deref()
            .context("HELIUS_API_KEY is required for this command (see .env.example)")
    }

    /// The shared secret, or a hard error naming the variable.
    pub fn required_webhook_secret(&self) -> Result<&str> {
        self.webhook_secret
            .as_deref()
            .context("HELIUS_WEBHOOK_SECRET is required for this command (see .env.example)")
    }

    /// The registered endpoint, or a hard error naming the variable.
    pub fn required_webhook_url(&self) -> Result<&str> {
        self.webhook_url
            .as_deref()
            .context("WEBHOOK_URL is required for this command (see .env.example)")
    }
}

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// `DATABASE_URL`. Optional at parse time (like `HELIUS_API_KEY`) so that
    /// tooling which never touches Postgres still boots; every binary that
    /// does talk to Postgres calls [`DatabaseConfig::required_url`] at
    /// startup. On Railway: `${{Postgres.DATABASE_URL}}`.
    pub url: Option<String>,
    /// `DATABASE_MAX_CONNECTIONS`, default 5 — Railway Postgres is shared by
    /// the api, the future ingester and one-off admin runs.
    pub max_connections: u32,
    /// `DATABASE_CONNECT_TIMEOUT_SECS`, default 5.
    pub connect_timeout_secs: u64,
}

impl DatabaseConfig {
    /// The URL, or a hard error naming the variable — services and the admin
    /// CLI call this at boot so a missing `DATABASE_URL` fails loudly.
    pub fn required_url(&self) -> Result<&str> {
        self.url
            .as_deref()
            .context("DATABASE_URL is required (see .env.example)")
    }
}

impl Config {
    pub fn try_from_env() -> Result<Self> {
        let database = DatabaseConfig {
            url: env::var("DATABASE_URL").ok().filter(|v| !v.is_empty()),
            max_connections: parsed_or("DATABASE_MAX_CONNECTIONS", 5)?,
            connect_timeout_secs: parsed_or("DATABASE_CONNECT_TIMEOUT_SECS", 5)?,
        };
        anyhow::ensure!(
            database.max_connections >= 1,
            "invalid DATABASE_MAX_CONNECTIONS=0: need at least one connection"
        );
        Ok(Self {
            server: ServerConfig {
                host: string_or("HOST", "::"),
                port: parsed_or("PORT", 8080)?,
            },
            helius: HeliusConfig {
                api_key: env::var("HELIUS_API_KEY").ok().filter(|v| !v.is_empty()),
                webhook_secret: env::var("HELIUS_WEBHOOK_SECRET")
                    .ok()
                    .filter(|v| !v.is_empty()),
                webhook_url: env::var("WEBHOOK_URL").ok().filter(|v| !v.is_empty()),
                webhook_api: string_or("HELIUS_WEBHOOK_API", "https://api-mainnet.helius-rpc.com"),
            },
            database,
            ingest: IngestConfig {
                transports: transports("INGEST_TRANSPORTS", "ws")?,
                webhook_poll_ms: parsed_or("WEBHOOK_POLL_MS", 1_000)?,
                webhook_batch: parsed_or("WEBHOOK_BATCH", 50)?,
                webhook_lease_secs: parsed_or("WEBHOOK_LEASE_SECS", 60)?,
                webhook_max_attempts: parsed_or("WEBHOOK_MAX_ATTEMPTS", 5)?,
                webhook_grace_secs: parsed_or("WEBHOOK_GRACE_SECS", 120)?,
                webhook_retain_days: parsed_or("WEBHOOK_RETAIN_DAYS", 30)?,
            },
            reconcile: ReconcileConfig {
                interval_secs: parsed_or("RECONCILE_INTERVAL_SECS", 3_600)?,
                deep_interval_secs: parsed_or("RECONCILE_DEEP_INTERVAL_SECS", 604_800)?,
                rps: parsed_or("RECONCILE_RPS", 10)?,
                tip_interval_secs: parsed_or("RECONCILE_TIP_INTERVAL_SECS", 30)?,
            },
            rarity: RarityConfig {
                interval_secs: parsed_or("RARITY_INTERVAL_SECS", 86_400)?,
            },
        })
    }
}

fn string_or(key: &str, default: &str) -> String {
    env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn parsed_or<T>(key: &str, default: T) -> Result<T>
where
    T: FromStr,
    T::Err: Display,
{
    match env::var(key) {
        Ok(raw) if !raw.is_empty() => raw
            .parse()
            .map_err(|e| anyhow::anyhow!("{e}"))
            .with_context(|| format!("invalid {key}={raw}")),
        _ => Ok(default),
    }
}

/// A comma-separated transport list. Empty is a hard error rather than a
/// silently inert ingester — a service that ingests nothing must say so at
/// boot, not look healthy while doing nothing.
fn transports(key: &str, default: &str) -> Result<Vec<Transport>> {
    let raw = string_or(key, default);
    let mut out = Vec::new();
    for member in raw.split(',').filter(|m| !m.trim().is_empty()) {
        let transport: Transport = member
            .parse()
            .with_context(|| format!("invalid {key}={raw}"))?;
        if !out.contains(&transport) {
            out.push(transport);
        }
    }
    if out.is_empty() {
        anyhow::bail!("invalid {key}={raw}: name at least one transport (ws | webhook)");
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEYS: [&str; 21] = [
        "HOST",
        "PORT",
        "HELIUS_API_KEY",
        "HELIUS_WEBHOOK_SECRET",
        "HELIUS_WEBHOOK_API",
        "WEBHOOK_URL",
        "DATABASE_URL",
        "DATABASE_MAX_CONNECTIONS",
        "DATABASE_CONNECT_TIMEOUT_SECS",
        "RECONCILE_INTERVAL_SECS",
        "RECONCILE_DEEP_INTERVAL_SECS",
        "RECONCILE_RPS",
        "RARITY_INTERVAL_SECS",
        "RECONCILE_TIP_INTERVAL_SECS",
        "INGEST_TRANSPORTS",
        "WEBHOOK_POLL_MS",
        "WEBHOOK_BATCH",
        "WEBHOOK_LEASE_SECS",
        "WEBHOOK_MAX_ATTEMPTS",
        "WEBHOOK_GRACE_SECS",
        "WEBHOOK_RETAIN_DAYS",
    ];

    fn clear() {
        for key in KEYS {
            env::remove_var(key);
        }
    }

    // One sequential test: env vars are process-global, so parallel tests
    // mutating the same keys would race.
    #[test]
    fn env_parsing() {
        clear();
        let config = Config::try_from_env().unwrap();
        assert_eq!(config.server.host, "::");
        assert_eq!(config.server.port, 8080);
        assert_eq!(config.helius.api_key, None);
        assert_eq!(config.database.url, None);
        assert_eq!(config.database.max_connections, 5);
        assert_eq!(config.database.connect_timeout_secs, 5);
        assert_eq!(config.rarity.interval_secs, 86_400);
        assert!(config.rarity.enabled());
        assert_eq!(config.reconcile.tip_interval_secs, 30);
        assert!(config.reconcile.tip_enabled());
        assert_eq!(config.helius.webhook_secret, None);
        assert_eq!(config.helius.webhook_url, None);
        assert_eq!(
            config.helius.webhook_api,
            "https://api-mainnet.helius-rpc.com"
        );
        // The WebSocket alone by default: adding the webhook lane is a
        // deliberate act, not something a deploy picks up on its own.
        assert_eq!(config.ingest.transports, vec![Transport::Ws]);
        assert!(config.ingest.runs(Transport::Ws));
        assert!(!config.ingest.runs(Transport::Webhook));
        assert_eq!(config.ingest.reconciler(), Some(Transport::Ws));
        assert_eq!(config.ingest.webhook_batch, 50);
        assert_eq!(config.ingest.webhook_max_attempts, 5);
        assert_eq!(config.ingest.webhook_grace_secs, 120);
        assert!(config
            .database
            .required_url()
            .unwrap_err()
            .to_string()
            .contains("DATABASE_URL"));

        env::set_var("PORT", "9090");
        env::set_var("HELIUS_API_KEY", "test-key");
        env::set_var("DATABASE_URL", "postgres://localhost/x");
        env::set_var("DATABASE_MAX_CONNECTIONS", "12");
        let config = Config::try_from_env().unwrap();
        assert_eq!(config.server.port, 9090);
        assert_eq!(config.helius.api_key.as_deref(), Some("test-key"));
        assert_eq!(
            config.database.required_url().unwrap(),
            "postgres://localhost/x"
        );
        assert_eq!(config.database.max_connections, 12);

        // Missing var -> default, present-but-unparseable -> hard error.
        env::set_var("RARITY_INTERVAL_SECS", "0");
        assert!(!Config::try_from_env().unwrap().rarity.enabled());
        env::set_var("RARITY_INTERVAL_SECS", "soon");
        let err = Config::try_from_env().unwrap_err();
        assert!(
            err.to_string().contains("RARITY_INTERVAL_SECS"),
            "unexpected error: {err:#}"
        );
        env::remove_var("RARITY_INTERVAL_SECS");

        env::set_var("PORT", "80800");
        let err = Config::try_from_env().unwrap_err();
        assert!(
            err.to_string().contains("PORT"),
            "unexpected error: {err:#}"
        );
        env::remove_var("PORT");

        env::set_var("DATABASE_MAX_CONNECTIONS", "abc");
        let err = Config::try_from_env().unwrap_err();
        assert!(
            err.to_string().contains("DATABASE_MAX_CONNECTIONS"),
            "unexpected error: {err:#}"
        );

        env::set_var("DATABASE_MAX_CONNECTIONS", "0");
        let err = Config::try_from_env().unwrap_err();
        assert!(
            err.to_string().contains("DATABASE_MAX_CONNECTIONS=0"),
            "unexpected error: {err:#}"
        );

        clear();
    }

    /// Constructed directly rather than through the environment: the env test
    /// above is deliberately sequential because env vars are process-global,
    /// and this needs no env at all.
    #[test]
    fn required_accessors_name_their_variable() {
        fn helius() -> HeliusConfig {
            HeliusConfig {
                api_key: None,
                webhook_secret: None,
                webhook_url: None,
                webhook_api: "https://example.invalid".into(),
            }
        }

        // Each accessor must name the variable a user has to set — the whole
        // point of deferring the check to the command that needs it.
        for (name, err) in [
            ("HELIUS_API_KEY", helius().required_api_key().unwrap_err()),
            (
                "HELIUS_WEBHOOK_SECRET",
                helius().required_webhook_secret().unwrap_err(),
            ),
            ("WEBHOOK_URL", helius().required_webhook_url().unwrap_err()),
        ] {
            assert!(
                err.to_string().contains(name),
                "the error for {name} must name it: {err:#}"
            );
        }

        let configured = HeliusConfig {
            api_key: Some("k".into()),
            webhook_secret: Some("s".into()),
            webhook_url: Some("https://example.test/webhooks/helius".into()),
            ..helius()
        };
        assert_eq!(configured.required_api_key().unwrap(), "k");
        assert_eq!(configured.required_webhook_secret().unwrap(), "s");
        assert_eq!(
            configured.required_webhook_url().unwrap(),
            "https://example.test/webhooks/helius"
        );
    }

    /// The retirement path is a single variable, and the reconcile owner moves
    /// with it — that is the whole reason `reconciler()` is derived rather than
    /// configured.
    #[test]
    fn transports_parse_and_carry_the_reconcile_owner() {
        assert_eq!(transports("X", "ws").unwrap(), vec![Transport::Ws]);

        env::set_var("X", "ws,webhook");
        let dual = IngestConfig {
            transports: transports("X", "ws").unwrap(),
            webhook_poll_ms: 1_000,
            webhook_batch: 50,
            webhook_lease_secs: 60,
            webhook_max_attempts: 5,
            webhook_grace_secs: 120,
            webhook_retain_days: 30,
        };
        assert_eq!(dual.transports, vec![Transport::Ws, Transport::Webhook]);
        assert_eq!(
            dual.reconciler(),
            Some(Transport::Ws),
            "during a dual run the WebSocket keeps the reconcile"
        );

        // Retirement: one variable, and the webhook lane inherits it.
        env::set_var("X", "webhook");
        let retired = IngestConfig {
            transports: transports("X", "ws").unwrap(),
            ..dual.clone()
        };
        assert_eq!(retired.reconciler(), Some(Transport::Webhook));
        assert!(!retired.runs(Transport::Ws));

        // Whitespace and duplicates are tolerated; unknown members are not,
        // because a typo would silently leave a transport unrun.
        env::set_var("X", " webhook , webhook ");
        assert_eq!(transports("X", "ws").unwrap(), vec![Transport::Webhook]);
        env::set_var("X", "ws,laserstream");
        let err = transports("X", "ws").unwrap_err();
        assert!(
            format!("{err:#}").contains("laserstream"),
            "the error must name the offending member: {err:#}"
        );
        env::remove_var("X");
    }
}
