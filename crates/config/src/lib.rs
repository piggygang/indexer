//! Environment-based configuration.
//!
//! A missing variable falls back to its default; a variable that is present
//! but unparseable is a hard error — a typo'd `PORT` must fail the boot, not
//! silently bind the default.
//!
//! The cluster is the one deliberate exception: `SOLANA_CLUSTER` has no
//! default, and the endpoints that reach it are never derived from it. See
//! [`SolanaConfig`].

use std::env;
use std::fmt::{self, Display};
use std::str::FromStr;

use anyhow::{Context, Result};
use url::{Host, Url};

#[derive(Debug, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub solana: SolanaConfig,
    pub helius: HeliusConfig,
    pub database: DatabaseConfig,
    pub ingest: IngestConfig,
    pub reconcile: ReconcileConfig,
    pub rarity: RarityConfig,
}

/// The Solana cluster one deployment indexes.
///
/// Config, not code: nothing in the pipeline branches on it. A deployment is
/// one cluster for its whole life — its own Railway environment, its own
/// Postgres — and what this value does is keep the things that name that
/// cluster from disagreeing with each other: the endpoints the binaries dial
/// ([`SolanaConfig`]), the `ingest_state` cursors the ingester resumes from,
/// and what `/health` tells someone looking at the wrong environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Cluster {
    MainnetBeta,
    Devnet,
}

impl Cluster {
    pub const ALL: [Cluster; 2] = [Cluster::MainnetBeta, Cluster::Devnet];

    /// The `SOLANA_CLUSTER` spelling, and what `/health` reports.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MainnetBeta => "mainnet-beta",
            Self::Devnet => "devnet",
        }
    }

    /// The cluster's genesis hash — what `getGenesisHash` answers on it.
    ///
    /// This is the cluster's identity on the wire, and the one thing an RPC
    /// endpoint cannot be wrong about by accident: a hostname is a label
    /// somebody typed, this is what the node itself says it is. The binaries
    /// that dial the RPC compare the two before they write anything.
    ///
    /// Not an on-chain *address* in the sense of the repo rule that keeps
    /// those in `config/`: no account lives here, nothing is tracked by it,
    /// and it cannot differ between deployments of the same cluster. It is
    /// what the word `devnet` means, so it sits next to the word. Both values
    /// were read back from the clusters' public RPCs on 2026-11-15.
    pub fn genesis_hash(self) -> &'static str {
        match self {
            Self::MainnetBeta => "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d",
            Self::Devnet => "EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG",
        }
    }
}

impl Display for Cluster {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Cluster {
    type Err = anyhow::Error;

    /// Exact spellings only. `mainnet` is the Wallet Standard's name for the
    /// same cluster and the likeliest typo, and it is refused like any other:
    /// a value that is *nearly* right must fail the boot, not be guessed at.
    fn from_str(value: &str) -> Result<Self> {
        match value.trim() {
            "mainnet-beta" => Ok(Self::MainnetBeta),
            "devnet" => Ok(Self::Devnet),
            other => anyhow::bail!("unknown cluster '{other}' (devnet | mainnet-beta)"),
        }
    }
}

/// The cluster and the endpoints that reach it.
///
/// **Explicit, never derived.** There is no `https://{cluster}.helius-rpc.com`
/// anywhere in this workspace: every endpoint is its own variable, so swapping
/// provider — or pointing one deployment at a dedicated node — is a config
/// change, and no binary can dial a host nobody wrote down.
///
/// The price of independent variables is that they can disagree, so agreement
/// is checked rather than assumed, twice:
///
/// * **Here, offline.** An endpoint whose hostname names another cluster
///   (`SOLANA_CLUSTER=devnet` with `https://mainnet.helius-rpc.com`) is a hard
///   error at parse time. This is the only check a WebSocket endpoint or the
///   webhook API host can get — neither answers `getGenesisHash`.
/// * **At run time, on the wire.** The ingester and the admin commands that
///   dial the RPC compare its `getGenesisHash` with
///   [`Cluster::genesis_hash`] before they write. A hostname with no cluster
///   in it (a dedicated node, a proxy) is only caught there.
#[derive(Debug, Clone)]
pub struct SolanaConfig {
    /// `SOLANA_CLUSTER`: `devnet` | `mainnet-beta`. **Required — the one
    /// variable in this crate with no default.** Every other knob has a value
    /// that is merely suboptimal when wrong; a defaulted cluster is a
    /// deployment that silently indexes the other chain into this database,
    /// and looks healthy while it does.
    pub cluster: Cluster,
    /// `SOLANA_RPC_URL` — JSON-RPC and DAS, e.g.
    /// `https://mainnet.helius-rpc.com`. The bare endpoint: `HELIUS_API_KEY`
    /// is appended as `?api-key=` by the client, so the key never has to be
    /// written into a variable that is declared in source.
    ///
    /// Optional at parse time, like `HELIUS_API_KEY`: the api never dials it,
    /// and `migrate`/`seed` must run without it. The binaries that do call
    /// [`SolanaConfig::required_rpc_url`].
    pub rpc_url: Option<String>,
    /// `SOLANA_WS_URL` — the Enhanced WebSocket endpoint, e.g.
    /// `wss://mainnet.helius-rpc.com`. Same shape and the same rule as
    /// [`SolanaConfig::rpc_url`], and deliberately its own variable rather
    /// than the RPC URL with the scheme swapped: that would be a derived URL.
    pub ws_url: Option<String>,
}

impl SolanaConfig {
    /// The RPC endpoint, or a hard error naming the variable.
    pub fn required_rpc_url(&self) -> Result<&str> {
        self.rpc_url.as_deref().context(
            "SOLANA_RPC_URL is required for this command (see .env.example) — \
             no endpoint is derived from SOLANA_CLUSTER",
        )
    }

    /// The WebSocket endpoint, or a hard error naming the variable.
    pub fn required_ws_url(&self) -> Result<&str> {
        self.ws_url.as_deref().context(
            "SOLANA_WS_URL is required to run the `ws` transport (see .env.example) — \
             no endpoint is derived from SOLANA_CLUSTER",
        )
    }

    /// One line for a boot log: the cluster and where each endpoint points.
    ///
    /// Origins only, never a path. Validation already refuses a query string
    /// and credentials, but a dedicated node's access token usually *is* the
    /// path, and a boot log is the first thing anyone pastes into an issue.
    pub fn describe(&self) -> String {
        format!(
            "{} (rpc {}, ws {})",
            self.cluster,
            origin_or_unset(self.rpc_url.as_deref()),
            origin_or_unset(self.ws_url.as_deref()),
        )
    }
}

/// `scheme://host[:port]` of a validated endpoint, for logs.
pub fn origin(endpoint: &str) -> String {
    Url::parse(endpoint)
        .map(|url| url.origin().ascii_serialization())
        .unwrap_or_else(|_| "<unparseable>".to_string())
}

fn origin_or_unset(endpoint: Option<&str>) -> String {
    endpoint.map(origin).unwrap_or_else(|| "unset".to_string())
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
    /// `HELIUS_WEBHOOK_API` — the webhook management API host, e.g.
    /// `https://api-mainnet.helius-rpc.com`. Helius's docs and their own SDK
    /// disagree on it: the docs say `mainnet.helius-rpc.com`, the SDK uses the
    /// `api-` one, and a legacy `api.helius.xyz` is still in circulation. The
    /// subcommand's first call is a free `GET`, so a wrong host fails cheaply
    /// and loudly.
    ///
    /// It used to default to the mainnet host. It no longer defaults at all:
    /// a webhook is registered *per cluster*, and a devnet deployment that
    /// inherited the mainnet host would spend 100 credits registering devnet
    /// addresses on mainnet. Same shape and same offline cluster check as the
    /// [`SolanaConfig`] endpoints; `indexer-admin webhook` calls
    /// [`HeliusConfig::required_webhook_api`].
    pub webhook_api: Option<String>,
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

    /// The webhook management API host, or a hard error naming the variable.
    pub fn required_webhook_api(&self) -> Result<&str> {
        self.webhook_api.as_deref().context(
            "HELIUS_WEBHOOK_API is required for this command (see .env.example) — \
             no endpoint is derived from SOLANA_CLUSTER",
        )
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
        // First, because every endpoint below is validated against it.
        let cluster = required_cluster("SOLANA_CLUSTER")?;
        Ok(Self {
            server: ServerConfig {
                host: string_or("HOST", "::"),
                port: parsed_or("PORT", 8080)?,
            },
            solana: SolanaConfig {
                cluster,
                rpc_url: endpoint("SOLANA_RPC_URL", Wire::Http, cluster)?,
                ws_url: endpoint("SOLANA_WS_URL", Wire::Ws, cluster)?,
            },
            helius: HeliusConfig {
                api_key: env::var("HELIUS_API_KEY").ok().filter(|v| !v.is_empty()),
                webhook_secret: env::var("HELIUS_WEBHOOK_SECRET")
                    .ok()
                    .filter(|v| !v.is_empty()),
                webhook_url: env::var("WEBHOOK_URL").ok().filter(|v| !v.is_empty()),
                webhook_api: endpoint("HELIUS_WEBHOOK_API", Wire::Http, cluster)?,
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

/// `SOLANA_CLUSTER`. The one variable with no default — see
/// [`SolanaConfig::cluster`].
fn required_cluster(key: &str) -> Result<Cluster> {
    match env::var(key) {
        Ok(raw) if !raw.is_empty() => raw.parse().with_context(|| format!("invalid {key}={raw}")),
        _ => anyhow::bail!(
            "{key} is required (devnet | mainnet-beta) and has no default: a defaulted \
             cluster would silently index the other chain into this database \
             (see .env.example)"
        ),
    }
}

/// Which scheme family an endpoint speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Wire {
    /// JSON-RPC, DAS and the webhook management API.
    Http,
    /// The WebSocket transport.
    Ws,
}

impl Wire {
    fn secure(self) -> &'static str {
        match self {
            Self::Http => "https",
            Self::Ws => "wss",
        }
    }

    fn plaintext(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Ws => "ws",
        }
    }
}

/// An endpoint variable: missing → `None`, present → validated, and an
/// invalid one is a hard error like every other variable here.
fn endpoint(key: &str, wire: Wire, cluster: Cluster) -> Result<Option<String>> {
    match env::var(key) {
        Ok(raw) if !raw.is_empty() => parse_endpoint(key, &raw, wire, cluster).map(Some),
        _ => Ok(None),
    }
}

/// Validates one endpoint and returns it normalized —
/// `scheme://host[:port][/path]`, no trailing slash — which is the shape the
/// clients append `/?api-key=…` to.
///
/// **No error here echoes the raw value.** Unlike `PORT`, a URL is where a key
/// ends up when someone pastes the line from the Helius dashboard, and the
/// commonest mistake this function exists to catch is exactly that paste. The
/// messages name the variable and the rule instead; the one that prints any of
/// it prints the origin — scheme and host — and only after a query string and
/// credentials have been ruled out.
fn parse_endpoint(key: &str, raw: &str, wire: Wire, cluster: Cluster) -> Result<String> {
    let raw = raw.trim();
    // Railway leaves a reference it cannot resolve in place as literal text —
    // the guard `DATABASE_URL` gets in data-model, for the same reason.
    anyhow::ensure!(
        !raw.contains("${{"),
        "invalid {key}: it still contains an unresolved Railway reference (`${{{{…}}}}`)"
    );
    let url = Url::parse(raw).map_err(|e| {
        anyhow::anyhow!(
            "invalid {key}: {e} — expected an absolute URL such as {}://<host>",
            wire.secure()
        )
    })?;

    let scheme = url.scheme();
    anyhow::ensure!(
        scheme == wire.secure() || scheme == wire.plaintext(),
        "invalid {key}: scheme `{scheme}://`, expected `{}://`",
        wire.secure()
    );
    let host = url
        .host()
        .with_context(|| format!("invalid {key}: no host"))?;
    // Every request carries the key in its query string, so plaintext to
    // anything but this machine would put it on the wire in the clear. A local
    // validator and the tests' loopback fakes are the only honest uses.
    anyhow::ensure!(
        scheme == wire.secure() || is_loopback(&host),
        "invalid {key}: plaintext `{scheme}://` is accepted only for a loopback host — \
         HELIUS_API_KEY travels in the URL of every request"
    );
    anyhow::ensure!(
        url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "invalid {key}: give the bare endpoint (scheme, host, optional port and path) — \
         the key belongs in HELIUS_API_KEY, which the client appends as `?api-key=`"
    );

    let origin = url.origin().ascii_serialization();
    let host = host.to_string();
    if let Some(named) = clusters_named_in(&host).find(|named| *named != cluster.as_str()) {
        anyhow::bail!(
            "{key}={origin} names {named}, but SOLANA_CLUSTER is {cluster} — \
             an endpoint is never derived from the cluster, so both must be set to agree"
        );
    }
    Ok(format!("{origin}{}", url.path().trim_end_matches('/')))
}

fn is_loopback(host: &Host<&str>) -> bool {
    match host {
        Host::Domain(name) => *name == "localhost",
        Host::Ipv4(ip) => ip.is_loopback(),
        Host::Ipv6(ip) => ip.is_loopback(),
    }
}

/// The clusters a hostname spells out: `devnet.helius-rpc.com`,
/// `api-devnet.helius-rpc.com`, `api.mainnet-beta.solana.com`,
/// `<id>-fast-mainnet.helius-rpc.com`.
///
/// Whole labels, split on `.` and `-` — never a substring, so
/// `devnetwork.example` names nothing. A host with no cluster in it also names
/// nothing and passes: this is a contradiction detector, not a whitelist, and
/// the on-chain `getGenesisHash` comparison is what covers those.
fn clusters_named_in(host: &str) -> impl Iterator<Item = &'static str> + '_ {
    host.split(['.', '-']).filter_map(|label| match label {
        "mainnet" => Some("mainnet-beta"),
        "devnet" => Some("devnet"),
        "testnet" => Some("testnet"),
        _ => None,
    })
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

    const KEYS: [&str; 24] = [
        "HOST",
        "PORT",
        "SOLANA_CLUSTER",
        "SOLANA_RPC_URL",
        "SOLANA_WS_URL",
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
        // The one variable with no default. Everything else has one, so this
        // is all an otherwise empty environment is missing — and it must say
        // so by name rather than boot as some cluster nobody chose.
        let err = Config::try_from_env().unwrap_err();
        assert!(
            err.to_string().contains("SOLANA_CLUSTER is required"),
            "unexpected error: {err:#}"
        );
        // Present-but-empty is missing, not a value: Railway and `.env` files
        // both produce it, and it must not parse as a cluster.
        env::set_var("SOLANA_CLUSTER", "");
        let err = Config::try_from_env().unwrap_err();
        assert!(
            err.to_string().contains("SOLANA_CLUSTER is required"),
            "unexpected error: {err:#}"
        );

        env::set_var("SOLANA_CLUSTER", "mainnet-beta");
        let config = Config::try_from_env().unwrap();
        assert_eq!(config.solana.cluster, Cluster::MainnetBeta);
        // No endpoint is derived from the cluster: unset stays unset, and the
        // binary that needs one is told which variable to set.
        assert_eq!(config.solana.rpc_url, None);
        assert_eq!(config.solana.ws_url, None);
        assert_eq!(config.helius.webhook_api, None);
        assert_eq!(
            config.solana.describe(),
            "mainnet-beta (rpc unset, ws unset)"
        );
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
        env::remove_var("DATABASE_MAX_CONNECTIONS");

        // The likeliest typo — the Wallet Standard's name for the same cluster
        // — is refused, and the error carries both the value and the fix.
        env::set_var("SOLANA_CLUSTER", "mainnet");
        let err = format!("{:#}", Config::try_from_env().unwrap_err());
        assert!(
            err.contains("SOLANA_CLUSTER=mainnet") && err.contains("devnet | mainnet-beta"),
            "unexpected error: {err}"
        );

        // A whole devnet configuration: every endpoint explicit, each one
        // normalized to the shape the clients append `/?api-key=` to.
        env::set_var("SOLANA_CLUSTER", "devnet");
        env::set_var("SOLANA_RPC_URL", "https://devnet.helius-rpc.com/");
        env::set_var("SOLANA_WS_URL", "wss://devnet.helius-rpc.com");
        env::set_var("HELIUS_WEBHOOK_API", "https://api-devnet.helius-rpc.com");
        let config = Config::try_from_env().unwrap();
        assert_eq!(config.solana.cluster, Cluster::Devnet);
        assert_eq!(
            config.solana.required_rpc_url().unwrap(),
            "https://devnet.helius-rpc.com"
        );
        assert_eq!(
            config.solana.required_ws_url().unwrap(),
            "wss://devnet.helius-rpc.com"
        );
        assert_eq!(
            config.helius.required_webhook_api().unwrap(),
            "https://api-devnet.helius-rpc.com"
        );
        assert_eq!(
            config.solana.describe(),
            "devnet (rpc https://devnet.helius-rpc.com, ws wss://devnet.helius-rpc.com)"
        );

        // The mistake two environments invite: one variable copied from the
        // other. Flip only the cluster and every endpoint now contradicts it,
        // so the boot fails on the first one instead of indexing devnet into
        // a mainnet database.
        env::set_var("SOLANA_CLUSTER", "mainnet-beta");
        let err = Config::try_from_env().unwrap_err().to_string();
        assert!(
            err.contains("SOLANA_RPC_URL") && err.contains("names devnet"),
            "unexpected error: {err}"
        );
        // Each endpoint is checked, not just the first.
        env::remove_var("SOLANA_RPC_URL");
        let err = Config::try_from_env().unwrap_err().to_string();
        assert!(err.contains("SOLANA_WS_URL"), "unexpected error: {err}");
        env::remove_var("SOLANA_WS_URL");
        let err = Config::try_from_env().unwrap_err().to_string();
        assert!(
            err.contains("HELIUS_WEBHOOK_API"),
            "unexpected error: {err}"
        );

        clear();
    }

    #[test]
    fn clusters_are_spelled_exactly() {
        for cluster in Cluster::ALL {
            assert_eq!(cluster.as_str().parse::<Cluster>().unwrap(), cluster);
            assert_eq!(cluster.to_string(), cluster.as_str());
        }
        assert_eq!(" devnet ".parse::<Cluster>().unwrap(), Cluster::Devnet);
        // Near misses are refused, never guessed at: `mainnet` is what the
        // Wallet Standard calls it, `testnet` is a real cluster this indexer
        // does not serve, and case matters because the value is also a label.
        for wrong in [
            "mainnet",
            "Mainnet-Beta",
            "DEVNET",
            "testnet",
            "localnet",
            "",
        ] {
            let err = wrong.parse::<Cluster>().unwrap_err().to_string();
            assert!(
                err.contains("devnet | mainnet-beta"),
                "the error for '{wrong}' must list the valid values: {err}"
            );
        }
    }

    /// The hashes are what `getGenesisHash` is compared against, so a typo
    /// here would make every boot fail — or, worse, make two clusters look
    /// alike. This cannot reach the network, so it pins the shape: 32 bytes of
    /// base58, and distinct.
    #[test]
    fn genesis_hashes_are_distinct_and_well_formed() {
        const BASE58: &str = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
        for cluster in Cluster::ALL {
            let hash = cluster.genesis_hash();
            assert!(
                (43..=44).contains(&hash.len()) && hash.chars().all(|c| BASE58.contains(c)),
                "{cluster}: {hash} is not a 32-byte base58 hash"
            );
        }
        assert_ne!(
            Cluster::MainnetBeta.genesis_hash(),
            Cluster::Devnet.genesis_hash()
        );
    }

    #[test]
    fn endpoints_are_validated_and_normalized() {
        let ok = |raw: &str, wire: Wire, cluster: Cluster| {
            parse_endpoint("SOLANA_RPC_URL", raw, wire, cluster).unwrap()
        };
        let main = Cluster::MainnetBeta;

        // The trailing slash goes, because the clients append `/?api-key=`.
        assert_eq!(
            ok("https://mainnet.helius-rpc.com/", Wire::Http, main),
            "https://mainnet.helius-rpc.com"
        );
        assert_eq!(
            ok(" wss://mainnet.helius-rpc.com ", Wire::Ws, main),
            "wss://mainnet.helius-rpc.com"
        );
        // A path survives — it is where a dedicated node keeps its token —
        // and a default port does not.
        assert_eq!(
            ok("https://rpc.example.test:443/v1/abc/", Wire::Http, main),
            "https://rpc.example.test/v1/abc"
        );
        // Plaintext is for this machine only: a local validator, a test fake.
        assert_eq!(
            ok("http://127.0.0.1:8899", Wire::Http, main),
            "http://127.0.0.1:8899"
        );
        assert_eq!(
            ok("ws://localhost:8900/", Wire::Ws, main),
            "ws://localhost:8900"
        );
        assert_eq!(
            ok("http://[::1]:8899", Wire::Http, main),
            "http://[::1]:8899"
        );

        let err = |raw: &str, wire: Wire| {
            parse_endpoint("SOLANA_RPC_URL", raw, wire, main)
                .unwrap_err()
                .to_string()
        };
        for (raw, wire, why) in [
            // No scheme: the commonest paste, and `Url` reads it as relative.
            ("mainnet.helius-rpc.com", Wire::Http, "absolute URL"),
            // The other variable's value.
            (
                "wss://mainnet.helius-rpc.com",
                Wire::Http,
                "expected `https://`",
            ),
            (
                "https://mainnet.helius-rpc.com",
                Wire::Ws,
                "expected `wss://`",
            ),
            // The key would cross the network in the clear.
            ("http://rpc.example.test", Wire::Http, "loopback"),
            ("ws://rpc.example.test", Wire::Ws, "loopback"),
            // The dashboard's copy button: the key belongs in its own variable.
            (
                "https://mainnet.helius-rpc.com/?api-key=s3cr3t",
                Wire::Http,
                "HELIUS_API_KEY",
            ),
            (
                "https://user:s3cr3t@rpc.example.test",
                Wire::Http,
                "HELIUS_API_KEY",
            ),
            (
                "https://rpc.example.test/#s3cr3t",
                Wire::Http,
                "HELIUS_API_KEY",
            ),
            (
                "${{Helius.RPC_URL}}",
                Wire::Http,
                "unresolved Railway reference",
            ),
        ] {
            let message = err(raw, wire);
            assert!(
                message.contains("SOLANA_RPC_URL") && message.contains(why),
                "{raw}: unexpected error: {message}"
            );
            // A URL is where a key ends up, so no error may repeat one.
            assert!(!message.contains("s3cr3t"), "{raw}: leaked: {message}");
        }
    }

    /// The offline half of "the cluster and its endpoints must agree" — and
    /// the only half the WebSocket and the webhook API host can get.
    #[test]
    fn an_endpoint_that_names_another_cluster_is_refused() {
        let check = |raw: &str, cluster: Cluster| {
            let wire = if raw.starts_with("ws") {
                Wire::Ws
            } else {
                Wire::Http
            };
            parse_endpoint("SOLANA_WS_URL", raw, wire, cluster).map_err(|e| e.to_string())
        };
        let (main, dev) = (Cluster::MainnetBeta, Cluster::Devnet);

        // Each cluster's own hosts, in every spelling Helius and Solana use.
        for raw in [
            "https://mainnet.helius-rpc.com",
            "wss://mainnet.helius-rpc.com",
            "https://api-mainnet.helius-rpc.com",
            "https://abc-456-fast-mainnet.helius-rpc.com",
            "https://api.mainnet-beta.solana.com",
        ] {
            assert!(check(raw, main).is_ok(), "{raw} is a mainnet-beta host");
            let err = check(raw, dev).unwrap_err();
            assert!(
                err.contains("names mainnet-beta") && err.contains("SOLANA_CLUSTER is devnet"),
                "{raw}: unexpected error: {err}"
            );
        }
        for raw in [
            "https://devnet.helius-rpc.com",
            "wss://devnet.helius-rpc.com",
            "https://api-devnet.helius-rpc.com",
            "https://123-xyz-fast-devnet.helius-rpc.com",
            "https://api.devnet.solana.com",
        ] {
            assert!(check(raw, dev).is_ok(), "{raw} is a devnet host");
            let err = check(raw, main).unwrap_err();
            assert!(
                err.contains("names devnet") && err.contains("SOLANA_CLUSTER is mainnet-beta"),
                "{raw}: unexpected error: {err}"
            );
        }
        // A cluster this indexer does not serve contradicts both.
        for cluster in Cluster::ALL {
            let err = check("https://api.testnet.solana.com", cluster).unwrap_err();
            assert!(err.contains("names testnet"), "unexpected error: {err}");
        }

        // A contradiction detector, not a whitelist: a host that names no
        // cluster passes for either one (the genesis-hash check is what covers
        // it), and a label is matched whole, never as a substring.
        for cluster in Cluster::ALL {
            for raw in [
                "https://rpc.example.test",
                "http://127.0.0.1:8899",
                "https://devnetwork.example.test",
                "https://mainnetwork.example.test",
            ] {
                assert!(check(raw, cluster).is_ok(), "{raw} names no cluster");
            }
        }
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
                webhook_api: None,
            }
        }
        fn solana() -> SolanaConfig {
            SolanaConfig {
                cluster: Cluster::Devnet,
                rpc_url: None,
                ws_url: None,
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
            (
                "HELIUS_WEBHOOK_API",
                helius().required_webhook_api().unwrap_err(),
            ),
            ("SOLANA_RPC_URL", solana().required_rpc_url().unwrap_err()),
            ("SOLANA_WS_URL", solana().required_ws_url().unwrap_err()),
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
            webhook_api: Some("https://api.example.test".into()),
        };
        assert_eq!(configured.required_api_key().unwrap(), "k");
        assert_eq!(configured.required_webhook_secret().unwrap(), "s");
        assert_eq!(
            configured.required_webhook_url().unwrap(),
            "https://example.test/webhooks/helius"
        );
        assert_eq!(
            configured.required_webhook_api().unwrap(),
            "https://api.example.test"
        );

        let configured = SolanaConfig {
            rpc_url: Some("https://rpc.example.test".into()),
            ws_url: Some("wss://ws.example.test".into()),
            ..solana()
        };
        assert_eq!(
            configured.required_rpc_url().unwrap(),
            "https://rpc.example.test"
        );
        assert_eq!(
            configured.required_ws_url().unwrap(),
            "wss://ws.example.test"
        );
        // The boot log names the origin and never the path: a dedicated
        // node's token usually *is* the path.
        let tokened = SolanaConfig {
            rpc_url: Some("https://rpc.example.test/t0k3n".into()),
            ..solana()
        };
        assert_eq!(
            tokened.describe(),
            "devnet (rpc https://rpc.example.test, ws unset)"
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
