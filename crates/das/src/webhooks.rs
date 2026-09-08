//! Helius webhook management — list, create, update, delete.
//!
//! Not DAS, and the crate name says otherwise. It lives here because this is
//! the workspace's Helius HTTP client in practice, and the rule that actually
//! matters is kept: **request/response only, never an `IngestSource`**. The
//! streaming interface is `crates/ingest`'s; a webhook's registration is a
//! configuration call, not a transport.
//!
//! Two facts shape the whole module:
//!
//! * **Every mutation costs 100 credits and rewrites the entire address list**
//!   — adding one mint is the same price as replacing all 17 000. So the caller
//!   lists first, diffs, and writes nothing when nothing changed. [`Diff`] is
//!   what makes "nothing changed" a checkable claim rather than a hope.
//! * **The base host is genuinely uncertain.** Helius's docs say
//!   `mainnet.helius-rpc.com`, their own SDK uses `api-mainnet.helius-rpc.com`,
//!   and a legacy `api.helius.xyz` is still in circulation. It is therefore
//!   configurable, and [`WebhookClient::list`] — a free `GET` — is the first
//!   call any caller makes, so a wrong host fails cheaply and by name.

use std::collections::BTreeSet;
use std::time::Duration;

use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::client::DasError;

/// Helius's cap on one webhook's `accountAddresses`. Our ~17 000 fit in one.
pub const MAX_ADDRESSES: usize = 100_000;

/// A registered webhook, as Helius reports it.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Webhook {
    #[serde(default)]
    pub webhook_id: String,
    #[serde(default)]
    pub webhook_url: String,
    #[serde(default)]
    pub webhook_type: String,
    #[serde(default)]
    pub account_addresses: Vec<String>,
    /// Helius does not return this on every route, and we never rely on
    /// reading it back — the secret is written, not verified, from here.
    #[serde(default)]
    pub auth_header: Option<String>,
}

/// What a sync would change. Empty means: do not spend the 100 credits.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Diff {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    /// The endpoint moved.
    pub url_changed: bool,
    /// We hold a secret and cannot prove the registered one matches, so a sync
    /// rewrites it whenever it rewrites anything else. Never a reason to write
    /// on its own — that would make every run cost 100 credits.
    pub kept: usize,
}

impl Diff {
    /// Between the tracked set and what Helius currently holds.
    pub fn between(registered: &Webhook, url: &str, wanted: &[String]) -> Self {
        let current: BTreeSet<&str> = registered
            .account_addresses
            .iter()
            .map(String::as_str)
            .collect();
        let wanted_set: BTreeSet<&str> = wanted.iter().map(String::as_str).collect();
        Self {
            added: wanted_set
                .difference(&current)
                .map(|a| (*a).to_string())
                .collect(),
            removed: current
                .difference(&wanted_set)
                .map(|a| (*a).to_string())
                .collect(),
            url_changed: registered.webhook_url != url,
            kept: current.intersection(&wanted_set).count(),
        }
    }

    pub fn is_noop(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && !self.url_changed
    }
}

pub struct WebhookClient {
    http: Client,
    base: String,
    api_key: String,
}

impl WebhookClient {
    /// `base` is the API host — see the module doc on why it is a parameter.
    pub fn new(base: &str, api_key: &str) -> Result<Self, DasError> {
        let http = Client::builder()
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(10))
            .user_agent(concat!("indexer-das/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| DasError::Transport(e.to_string()))?;
        Ok(Self {
            http,
            base: base.trim_end_matches('/').to_string(),
            api_key: api_key.to_string(),
        })
    }

    /// Never logged: the key is a credential and the URL carries it.
    fn url(&self, path: &str) -> String {
        format!("{}/v0/webhooks{path}?api-key={}", self.base, self.api_key)
    }

    /// Every registered webhook. A free `GET`, and the first call any sync
    /// makes — so a wrong host or key fails here, before anything is spent.
    pub async fn list(&self) -> Result<Vec<Webhook>, DasError> {
        let response = self
            .http
            .get(self.url(""))
            .send()
            .await
            .map_err(|e| DasError::Transport(e.to_string()))?;
        Self::decode(response, "listWebhooks").await
    }

    /// **Costs 100 credits.**
    pub async fn create(
        &self,
        url: &str,
        addresses: &[String],
        auth_header: &str,
    ) -> Result<Webhook, DasError> {
        let response = self
            .http
            .post(self.url(""))
            .json(&Self::body(url, addresses, auth_header))
            .send()
            .await
            .map_err(|e| DasError::Transport(e.to_string()))?;
        Self::decode(response, "createWebhook").await
    }

    /// **Costs 100 credits**, and rewrites the whole address list — Helius has
    /// no incremental add.
    pub async fn update(
        &self,
        webhook_id: &str,
        url: &str,
        addresses: &[String],
        auth_header: &str,
    ) -> Result<Webhook, DasError> {
        let response = self
            .http
            .put(self.url(&format!("/{webhook_id}")))
            .json(&Self::body(url, addresses, auth_header))
            .send()
            .await
            .map_err(|e| DasError::Transport(e.to_string()))?;
        Self::decode(response, "updateWebhook").await
    }

    /// **Costs 100 credits.**
    pub async fn delete(&self, webhook_id: &str) -> Result<(), DasError> {
        let response = self
            .http
            .delete(self.url(&format!("/{webhook_id}")))
            .send()
            .await
            .map_err(|e| DasError::Transport(e.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            return Err(DasError::Status {
                method: "deleteWebhook".into(),
                status,
            });
        }
        Ok(())
    }

    /// `raw`, deliberately.
    ///
    /// `enhanced` would deliver Helius's own interpreted schema, which the
    /// decoder cannot read at all, and it silently drops failed transactions.
    /// `raw` carries the `getTransaction`-shaped envelope this pipeline
    /// already understands. `transactionTypes` is omitted because raw webhooks
    /// cannot filter by type, and `txnStatus: "all"` keeps failures visible —
    /// the receiver records them as `failed` rather than pretending they did
    /// not happen.
    fn body(url: &str, addresses: &[String], auth_header: &str) -> serde_json::Value {
        json!({
            "webhookURL": url,
            "webhookType": "raw",
            "txnStatus": "all",
            "accountAddresses": addresses,
            "authHeader": auth_header,
        })
    }

    async fn decode<T: for<'de> Deserialize<'de>>(
        response: reqwest::Response,
        method: &str,
    ) -> Result<T, DasError> {
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| DasError::Transport(e.to_string()))?;
        if !status.is_success() {
            // The body often names the real problem (a bad host answers with
            // HTML, a bad key with JSON), and losing it would make a wrong
            // `HELIUS_WEBHOOK_API` unreadable.
            return Err(DasError::Rpc {
                method: method.into(),
                message: format!(
                    "HTTP {status}: {}",
                    body.chars().take(300).collect::<String>()
                ),
            });
        }
        serde_json::from_str(&body).map_err(|source| DasError::Decode {
            method: method.into(),
            source,
        })
    }
}

/// So a caller can special-case "no webhook registered yet" without matching
/// on strings.
pub fn is_not_found(error: &DasError) -> bool {
    matches!(error, DasError::Status { status, .. } if *status == StatusCode::NOT_FOUND)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn webhook(addresses: &[&str], url: &str) -> Webhook {
        Webhook {
            webhook_id: "wh_1".into(),
            webhook_url: url.into(),
            webhook_type: "raw".into(),
            account_addresses: addresses.iter().map(|a| (*a).to_string()).collect(),
            auth_header: None,
        }
    }

    /// The credit-saving claim, made checkable: an unchanged registry produces
    /// a no-op diff, so a scheduled sync spends nothing.
    #[test]
    fn an_unchanged_address_list_is_a_noop() {
        let registered = webhook(&["a", "b"], "https://x.test/hook");
        let diff = Diff::between(
            &registered,
            "https://x.test/hook",
            &["b".to_string(), "a".to_string()],
        );
        assert!(diff.is_noop(), "order must not count as a change: {diff:?}");
        assert_eq!(diff.kept, 2);
    }

    #[test]
    fn the_diff_names_both_directions_and_the_url() {
        let registered = webhook(&["a", "b"], "https://old.test/hook");
        let diff = Diff::between(
            &registered,
            "https://new.test/hook",
            &["b".to_string(), "c".to_string()],
        );
        assert_eq!(diff.added, vec!["c".to_string()]);
        assert_eq!(diff.removed, vec!["a".to_string()]);
        assert!(diff.url_changed);
        assert_eq!(diff.kept, 1);
        assert!(!diff.is_noop());
    }

    /// A moved endpoint alone must still be worth the write — otherwise a
    /// redeployed service would keep receiving nothing and look healthy.
    #[test]
    fn a_moved_url_alone_is_a_change() {
        let registered = webhook(&["a"], "https://old.test/hook");
        let diff = Diff::between(&registered, "https://new.test/hook", &["a".to_string()]);
        assert!(diff.added.is_empty() && diff.removed.is_empty());
        assert!(!diff.is_noop());
    }
}
