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
}

impl HeliusConfig {
    /// The key, or a hard error naming the variable — same idiom as
    /// [`DatabaseConfig::required_url`].
    pub fn required_api_key(&self) -> Result<&str> {
        self.api_key
            .as_deref()
            .context("HELIUS_API_KEY is required for this command (see .env.example)")
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
            },
            database,
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

#[cfg(test)]
mod tests {
    use super::*;

    const KEYS: [&str; 6] = [
        "HOST",
        "PORT",
        "HELIUS_API_KEY",
        "DATABASE_URL",
        "DATABASE_MAX_CONNECTIONS",
        "DATABASE_CONNECT_TIMEOUT_SECS",
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
    fn required_api_key_names_the_variable() {
        let err = HeliusConfig { api_key: None }
            .required_api_key()
            .unwrap_err();
        assert!(
            err.to_string().contains("HELIUS_API_KEY"),
            "unexpected error: {err:#}"
        );

        let helius = HeliusConfig {
            api_key: Some("k".into()),
        };
        assert_eq!(helius.required_api_key().unwrap(), "k");
    }
}
