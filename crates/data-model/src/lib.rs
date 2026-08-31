//! Postgres data model for the indexer (ALG-619): embedded migrations, the
//! connection pool, the typed collections registry, the seed loader, ingest
//! cursors and the facet queries the public API is built on.
//!
//! Conventions that every downstream issue relies on:
//!
//! - **Registry rows come only from `config/collections.toml`** via
//!   [`seed`]; membership is derived from the columns
//!   ([`types::MembershipRule`]), never from code.
//! - **Enums are `text + CHECK`** in Postgres and text-backed Rust enums
//!   here ([`types`]) — additive changes are one-line migrations.
//! - **Migrations are forward-only.** Never edit a file once it has been
//!   applied anywhere; add a new one.
//! - Queries are runtime-checked (`sqlx::query`/`query_as`), so neither the
//!   Docker build nor CI needs a database at compile time.

pub mod attributes;
pub mod facets;
pub mod ingest_state;
pub mod registry;
pub mod seed;
pub mod synth;
pub mod types;

use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::Context;
use sqlx::migrate::Migrator;
use sqlx::postgres::{PgConnectOptions, PgConnection, PgPoolOptions};
use sqlx::Connection;

pub use sqlx::PgPool;

/// Embedded `./migrations`. Applied by `indexer-api` at boot and by
/// `indexer-admin migrate`.
pub static MIGRATOR: Migrator = sqlx::migrate!();

const UNRESOLVED_REFERENCE: &str = "DATABASE_URL still contains an unresolved Railway reference \
(`${{…}}`) — set the variable to exactly `${{Postgres.DATABASE_URL}}`, nothing before or after it";

/// Validates `DATABASE_URL` before any connection attempt. Misconfigurations
/// that can never succeed fail immediately with a message naming the fix,
/// instead of burning the boot retry budget: an unresolved Railway
/// reference, or a `localhost` host inside a Railway container. Error
/// messages never include the password.
pub fn check_database_url(url: &str, on_railway: bool) -> anyhow::Result<PgConnectOptions> {
    if url.contains("${{") {
        return Err(anyhow::Error::msg(UNRESOLVED_REFERENCE));
    }
    let options = PgConnectOptions::from_str(url)
        .context("DATABASE_URL is not a valid postgres:// URL (see .env.example)")?;
    let host = options.get_host();
    if on_railway && matches!(host, "localhost" | "127.0.0.1" | "::1" | "[::1]") {
        anyhow::bail!(
            "DATABASE_URL points at {host}:{} inside a Railway container — set it to exactly \
             `${{{{Postgres.DATABASE_URL}}}}` (the Postgres service's private-network URL)",
            options.get_port()
        );
    }
    Ok(options)
}

/// `host:port/database as user` — what the boot log prints; never the password.
pub fn describe_database_target(options: &PgConnectOptions) -> String {
    format!(
        "{}:{}/{} as {}",
        options.get_host(),
        options.get_port(),
        options.get_database().unwrap_or("<default>"),
        options.get_username()
    )
}

/// True inside a Railway container. `RAILWAY_ENVIRONMENT` is a legacy alias
/// that is no longer in Railway's documented variable reference, so the
/// documented names are checked too — this guard must never fail open.
fn on_railway() -> bool {
    [
        "RAILWAY_ENVIRONMENT",
        "RAILWAY_ENVIRONMENT_NAME",
        "RAILWAY_SERVICE_ID",
    ]
    .iter()
    .any(|k| std::env::var_os(k).is_some())
}

/// Eager connect: fails fast when Postgres is unreachable, which is what a
/// CLI wants.
pub async fn connect(
    url: &str,
    max_connections: u32,
    connect_timeout: Duration,
) -> anyhow::Result<PgPool> {
    let options = check_database_url(url, on_railway())?;
    log::info!("database target: {}", describe_database_target(&options));
    PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(connect_timeout)
        .connect_with(options)
        .await
        .context("connecting to DATABASE_URL")
}

/// Version of the first migration; anything older in `_sqlx_migrations` is
/// another application's history (e.g. piggygang-services on port 5432).
const FIRST_MIGRATION: i64 = 20260829000100;

/// Applies pending migrations under sqlx's per-database advisory lock, so
/// the api and admin (and a future ingester) may race safely.
/// `ignore_missing` keeps an OLDER binary booting against a database that
/// already carries newer migrations — the README's `git revert` rollback —
/// which also disables sqlx's own "unknown history" check, hence the
/// explicit guard against migrating a foreign database.
pub async fn migrate(pool: &PgPool) -> anyhow::Result<()> {
    // `_sqlx_migrations` may not exist yet (fresh database): that is the one
    // error that means "no history"; anything else must surface.
    let foreign: Option<i64> = match sqlx::query_scalar::<_, Option<i64>>(
        "SELECT min(version) FROM _sqlx_migrations WHERE version < $1",
    )
    .bind(FIRST_MIGRATION)
    .fetch_one(pool)
    .await
    {
        Ok(min) => min,
        Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("42P01") => None,
        Err(e) => return Err(e).context("checking the migration history"),
    };
    if let Some(version) = foreign {
        anyhow::bail!(
            "DATABASE_URL points at a database with a foreign migration history \
             (version {version} predates this indexer's first migration); refusing to migrate"
        );
    }
    // Struct-literal over sqlx's `#[doc(hidden)]` (semver-exempt) fields —
    // revisit on any sqlx bump beyond 0.8.
    let migrator = Migrator {
        migrations: MIGRATOR.migrations.clone(),
        ignore_missing: true,
        ..Migrator::DEFAULT
    };
    migrator.run(pool).await.context("running migrations")
}

/// Boot-time connect with bounded retries: a Postgres restart or a brief
/// network blip must not turn into a crash loop under Railway's ON_FAILURE
/// restart policy, while a genuinely bad URL still fails within
/// `give_up_after`. The target is logged first (sanitized), each attempt is
/// a direct connection so the warning carries the real cause ("Connection
/// refused", DNS, auth) rather than the pool's acquire timeout, and URLs that
/// can never work are rejected without retrying. Migration failures are
/// never retried.
pub async fn connect_with_retry(
    url: &str,
    max_connections: u32,
    connect_timeout: Duration,
    give_up_after: Duration,
) -> anyhow::Result<PgPool> {
    let options = check_database_url(url, on_railway())?;
    let target = describe_database_target(&options);
    log::info!("database target: {target}");

    let started = Instant::now();
    let mut delay = Duration::from_secs(1);
    loop {
        let error =
            match tokio::time::timeout(connect_timeout, PgConnection::connect_with(&options)).await
            {
                Ok(Ok(conn)) => {
                    let _ = conn.close().await;
                    break;
                }
                Ok(Err(e)) => anyhow::Error::new(e),
                Err(_) => anyhow::anyhow!("connect timed out after {connect_timeout:?}"),
            };
        if started.elapsed() + delay >= give_up_after {
            return Err(error.context(format!("connecting to {target}")));
        }
        log::warn!("database not reachable ({error:#}); retrying in {delay:?}");
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(10));
    }
    // The probe proved reachability; the pool connects on first use (the
    // migration step right after boot).
    Ok(PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(connect_timeout)
        .connect_lazy_with(options))
}

/// `/ready` probe: `SELECT 1` bounded by a timeout.
pub async fn ping(pool: &PgPool, timeout: Duration) -> anyhow::Result<()> {
    tokio::time::timeout(timeout, sqlx::query("SELECT 1").execute(pool))
        .await
        .context("database ping timed out")?
        .context("database ping failed")?;
    Ok(())
}

/// Bench helper: rewrites 20% of a collection's asset rows without VACUUM,
/// clearing visibility-map bits so index-only scans degrade the way they do
/// under a live ingester. Never used by services.
pub async fn touch_assets_for_bench(pool: &PgPool, collection_id: i32) -> sqlx::Result<u64> {
    let result = sqlx::query(
        "UPDATE assets SET image_checked_at = now() WHERE collection_id = $1 AND id % 5 = 0",
    )
    .bind(collection_id)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOCAL: &str = "postgres://postgres:secret-pw@localhost:5433/piggygang_indexer";

    #[test]
    fn rejects_unresolved_railway_reference() {
        let url = LOCAL.to_string() + "${{Postgres.DATABASE_URL}}";
        let msg = check_database_url(&url, true).unwrap_err().to_string();
        assert!(msg.contains("unresolved"), "{msg}");
        assert!(!msg.contains("secret-pw"));
        // Also rejected with on_railway = false.
        assert!(check_database_url(&url, false).is_err());
    }

    #[test]
    fn rejects_localhost_inside_railway() {
        let msg = check_database_url(LOCAL, true).unwrap_err().to_string();
        assert!(msg.contains("localhost:5433"), "{msg}");
        assert!(msg.contains("${{Postgres.DATABASE_URL}}"), "{msg}");
        assert!(!msg.contains("secret-pw"));
    }

    #[test]
    fn accepts_localhost_locally_and_describes_without_password() {
        let options = check_database_url(LOCAL, false).unwrap();
        assert_eq!(
            describe_database_target(&options),
            "localhost:5433/piggygang_indexer as postgres"
        );
        let remote = check_database_url(
            "postgres://postgres:pw@postgres.railway.internal:5432/railway",
            true,
        )
        .unwrap();
        assert_eq!(
            describe_database_target(&remote),
            "postgres.railway.internal:5432/railway as postgres"
        );
    }

    #[test]
    fn rejects_garbage() {
        let msg = check_database_url("not a url", false)
            .unwrap_err()
            .to_string();
        assert!(msg.contains("not a valid postgres:// URL"), "{msg}");
    }

    #[tokio::test]
    async fn retry_gives_up_with_the_real_cause() {
        // Port 1 is closed: the probe fails immediately, the budget of 1 s
        // is exhausted before the first sleep, and the cause is preserved.
        let err = connect_with_retry(
            "postgres://x:y@127.0.0.1:1/x",
            1,
            Duration::from_secs(2),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("connecting to 127.0.0.1:1/x as x"), "{msg}");
        assert!(msg.to_lowercase().contains("refused"), "{msg}");
    }
}
