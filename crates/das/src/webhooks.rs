//! Helius webhook management — list, create, update, delete.
//!
//! Not DAS, and the crate name says otherwise. It lives here because this is
//! the workspace's Helius HTTP client in practice, and the rule that actually
//! matters is kept: **request/response only, never an `IngestSource`**. The
//! streaming interface is `crates/ingest`'s; a webhook's registration is a
//! configuration call, not a transport.
//!
//! Three facts shape the whole module:
//!
//! * **Every mutation costs 100 credits and rewrites the entire address list**
//!   — adding one mint is the same price as replacing all 17 000. So the caller
//!   lists first, diffs, and writes nothing when nothing changed. [`Diff`] is
//!   what makes "nothing changed" a checkable claim rather than a hope.
//! * **The base host is genuinely uncertain.** Helius's docs say
//!   `mainnet.helius-rpc.com`, their own SDK uses `api-mainnet.helius-rpc.com`,
//!   and a legacy `api.helius.xyz` is still in circulation. It is therefore
//!   configuration (`HELIUS_WEBHOOK_API`) with no default, and
//!   [`WebhookClient::list`], a free `GET`, is the first call any caller
//!   makes, so a wrong host fails cheaply and by name.
//! * **A webhook's cluster is its type, never the host.** Helius manages
//!   webhooks per account, through one API for every cluster (the
//!   `api-devnet` hosts answer 404 for `/v0/webhooks`), and a webhook watches
//!   devnet only when its `webhookType` is [`WebhookType::RawDevnet`]. A `raw`
//!   one over devnet addresses watches *mainnet* for them: 200s, no errors,
//!   nothing delivered. So the type is an argument to every write, and
//!   [`Diff`] counts a wrong one as a change.

use std::collections::BTreeSet;
use std::time::Duration;

use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::client::{http_client, mask, transport, DasError, Redirects};

/// Helius's cap on one webhook's `accountAddresses`. Our ~17 000 fit in one.
pub const MAX_ADDRESSES: usize = 100_000;

/// The `webhookType` a webhook is registered with, which is how Helius says
/// which cluster it watches — see the module doc. The raw kind on either
/// cluster, never `enhanced`: the request body's doc says why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebhookType {
    /// `raw`: mainnet-beta.
    Raw,
    /// `rawDevnet`: devnet.
    RawDevnet,
}

impl WebhookType {
    /// The spelling Helius takes and reports.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::RawDevnet => "rawDevnet",
        }
    }
}

impl std::fmt::Display for WebhookType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

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
    /// Registered with another cluster's type, so it watches the wrong chain.
    pub type_changed: bool,
    /// We hold a secret and cannot prove the registered one matches, so a sync
    /// rewrites it whenever it rewrites anything else. Never a reason to write
    /// on its own — that would make every run cost 100 credits.
    pub kept: usize,
}

impl Diff {
    /// Between the tracked set and what Helius currently holds.
    pub fn between(
        registered: &Webhook,
        url: &str,
        webhook_type: WebhookType,
        wanted: &[String],
    ) -> Self {
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
            // Compared only when Helius reported one, like the secret: an
            // unknown type must not turn every run into a 100-credit write.
            type_changed: !registered.webhook_type.is_empty()
                && registered.webhook_type != webhook_type.as_str(),
            kept: current.intersection(&wanted_set).count(),
        }
    }

    pub fn is_noop(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && !self.url_changed && !self.type_changed
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
        Ok(Self {
            // Every request here carries the key in its query, and `create`
            // and `update` carry the webhook secret in their body, so this
            // client never follows a redirect — see `Redirects::Refuse`.
            http: http_client(Duration::from_secs(60), Redirects::Refuse)?,
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
            .map_err(transport)?;
        self.decode(response, "listWebhooks", None).await
    }

    /// **Costs 100 credits.**
    pub async fn create(
        &self,
        url: &str,
        webhook_type: WebhookType,
        addresses: &[String],
        auth_header: &str,
    ) -> Result<Webhook, DasError> {
        let response = self
            .http
            .post(self.url(""))
            .json(&Self::body(url, webhook_type, addresses, auth_header))
            .send()
            .await
            .map_err(transport)?;
        self.decode(response, "createWebhook", Some(auth_header))
            .await
    }

    /// **Costs 100 credits**, and rewrites the whole address list — Helius has
    /// no incremental add.
    pub async fn update(
        &self,
        webhook_id: &str,
        url: &str,
        webhook_type: WebhookType,
        addresses: &[String],
        auth_header: &str,
    ) -> Result<Webhook, DasError> {
        let response = self
            .http
            .put(self.url(&format!("/{webhook_id}")))
            .json(&Self::body(url, webhook_type, addresses, auth_header))
            .send()
            .await
            .map_err(transport)?;
        self.decode(response, "updateWebhook", Some(auth_header))
            .await
    }

    /// **Costs 100 credits.**
    pub async fn delete(&self, webhook_id: &str) -> Result<(), DasError> {
        let response = self
            .http
            .delete(self.url(&format!("/{webhook_id}")))
            .send()
            .await
            .map_err(transport)?;
        let status = response.status();
        if !status.is_success() {
            return Err(DasError::Status {
                method: "deleteWebhook".into(),
                status,
            });
        }
        Ok(())
    }

    /// `raw` (or its devnet twin), deliberately.
    ///
    /// `enhanced` would deliver Helius's own interpreted schema, which the
    /// decoder cannot read at all, and it silently drops failed transactions.
    /// `raw` carries the `getTransaction`-shaped envelope this pipeline
    /// already understands. `transactionTypes` is omitted because raw webhooks
    /// cannot filter by type, and `txnStatus: "all"` keeps failures visible —
    /// the receiver records them as `failed` rather than pretending they did
    /// not happen.
    fn body(
        url: &str,
        webhook_type: WebhookType,
        addresses: &[String],
        auth_header: &str,
    ) -> serde_json::Value {
        json!({
            "webhookURL": url,
            "webhookType": webhook_type.as_str(),
            "txnStatus": "all",
            "accountAddresses": addresses,
            "authHeader": auth_header,
        })
    }

    /// `auth_header` is the webhook secret the request carried in its body,
    /// when it carried one.
    async fn decode<T: for<'de> Deserialize<'de>>(
        &self,
        response: reqwest::Response,
        method: &str,
        auth_header: Option<&str>,
    ) -> Result<T, DasError> {
        let status = response.status();
        let body = response.text().await.map_err(transport)?;
        if !status.is_success() {
            // The body often names the real problem (a bad host answers with
            // HTML, a bad key with JSON), and losing it would make a wrong
            // `HELIUS_WEBHOOK_API` unreadable. It is somebody else's text,
            // though, and a validation error that repeats what it was sent
            // repeats the key and the webhook secret with it. Both are masked
            // by value — and before the cut, so a secret that straddles the
            // 300th character does not survive as its first half.
            let masked = mask(&body, &[&self.api_key, auth_header.unwrap_or_default()]);
            let excerpt: String = masked.chars().take(300).collect();
            return Err(DasError::Rpc {
                method: method.into(),
                message: format!("HTTP {status}: {excerpt}"),
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
            WebhookType::Raw,
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
            WebhookType::Raw,
            &["b".to_string(), "c".to_string()],
        );
        assert_eq!(diff.added, vec!["c".to_string()]);
        assert_eq!(diff.removed, vec!["a".to_string()]);
        assert!(diff.url_changed);
        assert!(!diff.type_changed);
        assert_eq!(diff.kept, 1);
        assert!(!diff.is_noop());
    }

    /// A webhook over devnet addresses registered as `raw` watches mainnet
    /// for them and delivers nothing while looking healthy — so the type goes
    /// out on every write, and a wrong one alone is worth the 100 credits.
    #[test]
    fn the_type_says_which_cluster_and_a_wrong_one_is_a_change() {
        let body = WebhookClient::body(
            "https://x.test/hook",
            WebhookType::RawDevnet,
            &["a".to_string()],
            "secret",
        );
        assert_eq!(body["webhookType"], "rawDevnet");
        assert_eq!(
            WebhookClient::body("https://x.test/hook", WebhookType::Raw, &[], "secret")
                ["webhookType"],
            "raw"
        );

        let registered = webhook(&["a"], "https://x.test/hook");
        let diff = Diff::between(
            &registered,
            "https://x.test/hook",
            WebhookType::RawDevnet,
            &["a".to_string()],
        );
        assert!(diff.type_changed && !diff.url_changed && diff.added.is_empty());
        assert!(!diff.is_noop());

        // A type Helius did not report is not evidence of a wrong one; like
        // the secret, it must not make every run a write.
        let unreported = Webhook {
            webhook_type: String::new(),
            ..registered
        };
        let diff = Diff::between(
            &unreported,
            "https://x.test/hook",
            WebhookType::RawDevnet,
            &["a".to_string()],
        );
        assert!(diff.is_noop(), "{diff:?}");
    }

    /// The management URL carries the key exactly as the RPC one does, and a
    /// wrong `HELIUS_WEBHOOK_API` host is the commonest way to see a transport
    /// error here — printed, chain and all, on the operator's terminal.
    #[tokio::test]
    async fn a_transport_error_never_carries_the_api_key() {
        let client = WebhookClient::new("http://127.0.0.1:1", "s3cr3t-key").unwrap();
        let error = client.list().await.unwrap_err();
        let rendered = format!("{error} / {error:?}");
        assert!(!rendered.contains("s3cr3t-key"), "leaked: {rendered}");
    }

    /// A `400` that repeats the request it rejected — the request line with the
    /// key in it, the JSON body with the webhook secret in it — is the likeliest
    /// way for either to end up on a terminal or in a log. The diagnostic
    /// survives; the two values do not.
    #[tokio::test]
    async fn an_error_body_that_echoes_the_request_is_masked() {
        use crate::client::test_support::answering_with;

        let base = answering_with("400 Bad Request", |request| {
            let (head, body) = request.split_once("\r\n\r\n").unwrap_or((request, ""));
            let line = head.lines().next().unwrap_or_default();
            format!("invalid payload {body} for {line}")
        })
        .await;
        let client = WebhookClient::new(&base, "s3cr3t-key-value").unwrap();

        let error = client
            .create(
                "https://example.test/hook",
                WebhookType::Raw,
                &["a".to_string()],
                "wh-s3cr3t-value",
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("400") && error.contains("https://example.test/hook"),
            "the diagnostic must survive: {error}"
        );
        assert!(
            error.contains(r#""authHeader":"***""#) && error.contains("api-key=***"),
            "both were echoed, so both must show as masked: {error}"
        );
        assert!(!error.contains("s3cr3t"), "leaked: {error}");

        // A short secret with a quote and a backslash in it: it goes out
        // JSON-escaped, so the echo does not contain its raw bytes — and it is
        // still the secret, in a spelling anyone can unescape.
        let base = answering_with("400 Bad Request", |request| {
            let body = request.split_once("\r\n\r\n").map_or("", |(_, body)| body);
            format!("invalid payload {body}")
        })
        .await;
        let client = WebhookClient::new(&base, "s3cr3t-key-value").unwrap();
        let secret = r#"h"k\1"#;
        let error = client
            .create(
                "https://example.test/hook",
                WebhookType::Raw,
                &["a".to_string()],
                secret,
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(r#""authHeader":"***""#),
            "the echoed secret must show as masked: {error}"
        );
        assert!(
            !error.contains(secret) && !error.contains(r#"h\"k\\1"#),
            "leaked, raw or escaped: {error}"
        );
    }

    /// Every request here carries the key in its query, and `create` and
    /// `update` carry the webhook secret in their body — which a `307` or
    /// `308` asks the client to repeat, verbatim, at another origin. So none
    /// of them follows a redirect: the next host hears nothing at all.
    #[tokio::test]
    async fn a_redirect_is_refused_so_neither_the_key_nor_the_secret_travels_on() {
        use crate::client::test_support::redirect_to_another_origin;

        // `list`, a GET: the key is in the query.
        let (base, mut seen) = redirect_to_another_origin("[]").await;
        let client = WebhookClient::new(&base, "s3cr3t-key").unwrap();
        let error = client.list().await.unwrap_err().to_string();
        assert!(error.contains("307"), "unexpected error: {error}");
        assert!(seen.try_recv().is_err(), "the redirect was followed");

        // `create`, a POST: the body is what a 307 would have repeated.
        let (base, mut seen) = redirect_to_another_origin(r#"{"webhookID":"wh_1"}"#).await;
        let client = WebhookClient::new(&base, "s3cr3t-key").unwrap();
        let error = client
            .create(
                "https://example.test/hook",
                WebhookType::Raw,
                &["a".to_string()],
                "wh-s3cr3t",
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("307"), "unexpected error: {error}");
        assert!(
            !error.contains("s3cr3t"),
            "the error must not carry the key or the secret: {error}"
        );
        assert!(
            seen.try_recv().is_err(),
            "the body — and the webhook secret in it — was repeated to another origin"
        );
    }

    /// A moved endpoint alone must still be worth the write — otherwise a
    /// redeployed service would keep receiving nothing and look healthy.
    #[test]
    fn a_moved_url_alone_is_a_change() {
        let registered = webhook(&["a"], "https://old.test/hook");
        let diff = Diff::between(
            &registered,
            "https://new.test/hook",
            WebhookType::Raw,
            &["a".to_string()],
        );
        assert!(diff.added.is_empty() && diff.removed.is_empty());
        assert!(!diff.is_noop());
    }
}
