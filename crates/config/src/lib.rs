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
    /// Not needed by the API service yet; ingest/backfill work (ALG-621/623)
    /// validates its presence where required.
    pub api_key: Option<String>,
}

impl Config {
    pub fn try_from_env() -> Result<Self> {
        Ok(Self {
            server: ServerConfig {
                host: string_or("HOST", "::"),
                port: parsed_or("PORT", 8080)?,
            },
            helius: HeliusConfig {
                api_key: env::var("HELIUS_API_KEY").ok().filter(|v| !v.is_empty()),
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

#[cfg(test)]
mod tests {
    use super::*;

    // One sequential test: env vars are process-global, so parallel tests
    // mutating the same keys would race.
    #[test]
    fn env_parsing() {
        env::remove_var("HOST");
        env::remove_var("PORT");
        env::remove_var("HELIUS_API_KEY");
        let config = Config::try_from_env().unwrap();
        assert_eq!(config.server.host, "::");
        assert_eq!(config.server.port, 8080);
        assert_eq!(config.helius.api_key, None);

        env::set_var("PORT", "9090");
        env::set_var("HELIUS_API_KEY", "test-key");
        let config = Config::try_from_env().unwrap();
        assert_eq!(config.server.port, 9090);
        assert_eq!(config.helius.api_key.as_deref(), Some("test-key"));

        env::set_var("PORT", "80800");
        let err = Config::try_from_env().unwrap_err();
        assert!(
            err.to_string().contains("PORT"),
            "unexpected error: {err:#}"
        );

        env::remove_var("PORT");
        env::remove_var("HELIUS_API_KEY");
    }
}
