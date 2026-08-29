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

use std::time::Duration;

use anyhow::Context;
use sqlx::migrate::Migrator;
use sqlx::postgres::PgPoolOptions;

pub use sqlx::PgPool;

/// Embedded `./migrations`. Applied by `indexer-api` at boot and by
/// `indexer-admin migrate`.
pub static MIGRATOR: Migrator = sqlx::migrate!();

/// Eager connect: fails fast when Postgres is unreachable, which is what a
/// service wants at boot.
pub async fn connect(
    url: &str,
    max_connections: u32,
    connect_timeout: Duration,
) -> anyhow::Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(connect_timeout)
        .connect(url)
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

/// [`connect`] with bounded retries for boot time: a Postgres restart or a
/// brief network blip must not turn into a crash loop under Railway's
/// ON_FAILURE restart policy, while a genuinely bad URL still fails within
/// `give_up_after`. Migration failures are never retried.
pub async fn connect_with_retry(
    url: &str,
    max_connections: u32,
    connect_timeout: Duration,
    give_up_after: Duration,
) -> anyhow::Result<PgPool> {
    let started = std::time::Instant::now();
    let mut delay = Duration::from_secs(1);
    loop {
        match connect(url, max_connections, connect_timeout).await {
            Ok(pool) => return Ok(pool),
            Err(e) if started.elapsed() + delay < give_up_after => {
                log::warn!("database not reachable ({e:#}); retrying in {delay:?}");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(10));
            }
            Err(e) => return Err(e),
        }
    }
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
