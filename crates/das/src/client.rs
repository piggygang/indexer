//! Helius DAS over JSON-RPC, plus the plain HTTPS fetches for off-chain
//! metadata and image reachability.
//!
//! Hand-rolled rather than the `helius` SDK: DAS is a handful of JSON-RPC
//! POSTs, while the SDK hard-depends on the whole `solana-sdk`/`solana-client`
//! tree and defaults to `native-tls`, against this repo's rustls-only policy.

use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::asset::Asset;

/// DAS caps `getAssetBatch` at 1 000 ids.
pub const MAX_BATCH: usize = 1000;

const DEFAULT_ENDPOINT: &str = "https://mainnet.helius-rpc.com";

#[derive(Debug, thiserror::Error)]
pub enum DasError {
    #[error("http transport: {0}")]
    Transport(String),
    #[error("{method} returned HTTP {status}")]
    Status { method: String, status: StatusCode },
    #[error("{method} returned a JSON-RPC error: {message}")]
    Rpc { method: String, message: String },
    #[error("malformed {method} response: {0}", .source)]
    Decode {
        method: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("getAssetBatch accepts at most {MAX_BATCH} ids, got {0}")]
    BatchTooLarge(usize),
}

/// What one `getAssetBatch` produced. Assets are matched **by id**, never by
/// position — DAS is under no obligation to preserve request order.
#[derive(Debug, Default)]
pub struct BatchResult {
    pub found: Vec<Asset>,
    /// Requested ids DAS does not know. Never silently dropped: the run
    /// reports them so nobody mistakes a short collection for a complete one.
    pub missing: Vec<String>,
}

/// One entry of `getSignaturesForAddress`.
#[derive(Debug, Clone, Deserialize)]
pub struct SignatureInfo {
    pub signature: String,
    pub slot: i64,
    /// Unix seconds. The RPC spells it `blockTime`; without the rename this
    /// would silently stay `None` on every row.
    #[serde(rename = "blockTime", default)]
    pub block_time: Option<i64>,
    /// Present when the transaction failed on chain.
    #[serde(default)]
    pub err: Option<Value>,
}

impl SignatureInfo {
    pub fn failed(&self) -> bool {
        self.err.is_some()
    }

    pub fn block_time_utc(&self) -> Option<DateTime<Utc>> {
        self.block_time
            .and_then(|secs| Utc.timestamp_opt(secs, 0).single())
    }
}

#[derive(Debug, Default)]
pub struct SearchPage {
    pub items: Vec<Asset>,
    /// Present only when `showGrandTotal` was requested (page 1).
    pub grand_total: Option<u64>,
}

/// Outcome of an image probe. `Undetermined` deliberately has no database
/// representation — the columns are left untouched so the next pass retries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reachability {
    Ok,
    Dead,
    Undetermined,
}

#[derive(Clone)]
pub struct DasClient {
    http: Client,
    endpoint: String,
    api_key: String,
    max_attempts: u32,
}

impl DasClient {
    pub fn new(api_key: &str) -> Result<Self, DasError> {
        Self::with_endpoint(DEFAULT_ENDPOINT, api_key)
    }

    /// Points the client at an arbitrary endpoint — the integration test
    /// serves a fake Helius from loopback, so no key and no network are
    /// needed to exercise the whole pipeline.
    pub fn with_endpoint(endpoint: &str, api_key: &str) -> Result<Self, DasError> {
        let http = Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .user_agent(concat!("indexer-das/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| DasError::Transport(e.to_string()))?;
        Ok(Self {
            http,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            api_key: api_key.to_string(),
            max_attempts: 4,
        })
    }

    pub fn with_max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = attempts.max(1);
        self
    }

    /// The RPC URL. Never logged — see [`redact`].
    fn url(&self) -> String {
        if self.api_key.is_empty() {
            self.endpoint.clone()
        } else {
            format!("{}/?api-key={}", self.endpoint, self.api_key)
        }
    }

    /// Current slot. Read **before** each data call, so the value stamped on
    /// `assets.owner_slot` is a conservative lower bound on the observation.
    pub async fn get_slot(&self) -> Result<i64, DasError> {
        let result = self.rpc("getSlot", json!([])).await?;
        result.as_i64().ok_or_else(|| DasError::Rpc {
            method: "getSlot".into(),
            message: format!("expected a number, got {result}"),
        })
    }

    /// Wall-clock time of a slot. `None` when the cluster does not have it —
    /// a very fresh `confirmed` slot, or one outside the RPC's block window.
    ///
    /// `activity.block_time` is NOT NULL, so an event whose slot cannot be
    /// resolved must be parked rather than guessed at; see
    /// `indexer_data_model::activity::park_signature`.
    pub async fn get_block_time(&self, slot: i64) -> Result<Option<DateTime<Utc>>, DasError> {
        let result = self.rpc("getBlockTime", json!([slot])).await?;
        Ok(result
            .as_i64()
            .and_then(|secs| Utc.timestamp_opt(secs, 0).single()))
    }

    /// Signatures that reference an address, newest first.
    ///
    /// This is the gap-recovery primitive: it is 1 credit rather than DAS's
    /// 10, it works for regular NFTs (DAS's `getSignaturesForAsset` is
    /// documented for *compressed* assets), and it carries `blockTime`, which
    /// removes the need for a separate `getBlockTime` on the recovery path.
    pub async fn get_signatures_for_address(
        &self,
        address: &str,
        before: Option<&str>,
        limit: u32,
    ) -> Result<Vec<SignatureInfo>, DasError> {
        let mut options = json!({ "limit": limit, "commitment": "confirmed" });
        if let Some(before) = before {
            options["before"] = json!(before);
        }
        let value = self
            .rpc("getSignaturesForAddress", json!([address, options]))
            .await?;
        serde_json::from_value(value).map_err(|source| DasError::Decode {
            method: "getSignaturesForAddress".into(),
            source,
        })
    }

    /// One transaction in the same `jsonParsed` shape the WebSocket delivers,
    /// so the recovery path can feed the *same* decoder as the live path.
    /// `None` when the cluster no longer has it.
    pub async fn get_transaction(&self, signature: &str) -> Result<Option<Value>, DasError> {
        let params = json!([
            signature,
            {
                "encoding": "jsonParsed",
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0,
            }
        ]);
        let value = self.rpc("getTransaction", params).await?;
        Ok((!value.is_null()).then_some(value))
    }

    /// Fetches up to [`MAX_BATCH`] assets, splitting the chunk when DAS
    /// rejects it wholesale for containing an unknown id.
    pub async fn get_asset_batch(&self, ids: &[String]) -> Result<BatchResult, DasError> {
        if ids.len() > MAX_BATCH {
            return Err(DasError::BatchTooLarge(ids.len()));
        }
        self.batch_with_bisection(ids).await
    }

    /// DAS reports unknown ids two ways depending on deployment: `null`
    /// entries in the array, or HTTP 404 for the whole batch. Nulls are
    /// diffed by id; a 404 is bisected down to size 1, which isolates the
    /// offenders in ~`k·log₂(n)` extra calls rather than re-querying every id
    /// individually.
    fn batch_with_bisection<'a>(
        &'a self,
        ids: &'a [String],
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<BatchResult, DasError>> + Send + 'a>,
    > {
        Box::pin(async move {
            if ids.is_empty() {
                return Ok(BatchResult::default());
            }
            let params = json!({
                "ids": ids,
                "options": {"showUnverifiedCollections": true, "showCollectionMetadata": false},
            });
            match self.rpc("getAssetBatch", params).await {
                Ok(value) => {
                    let found: Vec<Asset> = serde_json::from_value::<Vec<Option<Asset>>>(value)
                        .map_err(|source| DasError::Decode {
                            method: "getAssetBatch".into(),
                            source,
                        })?
                        .into_iter()
                        .flatten()
                        .filter(|asset| !asset.id.is_empty())
                        .collect();
                    let returned: std::collections::HashSet<&str> =
                        found.iter().map(|a| a.id.as_str()).collect();
                    let missing = ids
                        .iter()
                        .filter(|id| !returned.contains(id.as_str()))
                        .cloned()
                        .collect();
                    Ok(BatchResult { found, missing })
                }
                Err(DasError::Status { status, .. }) if status == StatusCode::NOT_FOUND => {
                    if ids.len() == 1 {
                        return Ok(BatchResult {
                            found: Vec::new(),
                            missing: ids.to_vec(),
                        });
                    }
                    let (left, right) = ids.split_at(ids.len() / 2);
                    let mut result = self.batch_with_bisection(left).await?;
                    let rest = self.batch_with_bisection(right).await?;
                    result.found.extend(rest.found);
                    result.missing.extend(rest.missing);
                    Ok(result)
                }
                Err(other) => Err(other),
            }
        })
    }

    /// One page of a collection's assets. Used for both `core_collection` and
    /// `tm_collection` — the only difference between them is which address
    /// the registry supplies.
    pub async fn search_assets(
        &self,
        collection: &str,
        page: u32,
        limit: u32,
        grand_total: bool,
    ) -> Result<SearchPage, DasError> {
        let params = json!({
            "grouping": ["collection", collection],
            "page": page,
            "limit": limit,
            "sortBy": {"sortBy": "id", "sortDirection": "asc"},
            "options": {"showUnverifiedCollections": true, "showGrandTotal": grand_total},
        });
        let value = self.rpc("searchAssets", params).await?;

        #[derive(Deserialize)]
        struct Page {
            #[serde(default)]
            items: Vec<Asset>,
            #[serde(default)]
            grand_total: Option<u64>,
            #[serde(default)]
            total: Option<u64>,
        }
        let page: Page = serde_json::from_value(value).map_err(|source| DasError::Decode {
            method: "searchAssets".into(),
            source,
        })?;
        Ok(SearchPage {
            items: page.items,
            grand_total: page.grand_total.or(page.total),
        })
    }

    /// A JSON-RPC call with retries. Returns the `result` member.
    async fn rpc(&self, method: &str, params: Value) -> Result<Value, DasError> {
        let body = json!({"jsonrpc": "2.0", "id": "indexer", "method": method, "params": params});
        let url = self.url();

        let mut attempt = 1;
        loop {
            let response = self.http.post(&url).json(&body).send().await;
            let outcome: Result<Value, DasError> = match response {
                Ok(response) => {
                    let status = response.status();
                    if status.is_success() {
                        match response.json::<RpcResponse>().await {
                            Ok(RpcResponse {
                                error: Some(error), ..
                            }) => {
                                // A JSON-RPC error is the server answering,
                                // not a transport fault: never retried.
                                return Err(DasError::Rpc {
                                    method: method.into(),
                                    message: error.message,
                                });
                            }
                            Ok(RpcResponse { result, .. }) => {
                                return Ok(result.unwrap_or(Value::Null))
                            }
                            Err(e) => Err(DasError::Transport(e.to_string())),
                        }
                    } else {
                        Err(DasError::Status {
                            method: method.into(),
                            status,
                        })
                    }
                }
                Err(e) => Err(DasError::Transport(e.to_string())),
            };

            let error = outcome.unwrap_err();
            if attempt >= self.max_attempts || !is_retryable(&error) {
                return Err(error);
            }
            log::debug!(
                "{method} attempt {attempt}/{} failed against {}: {error}",
                self.max_attempts,
                redact(&url)
            );
            tokio::time::sleep(backoff(attempt)).await;
            attempt += 1;
        }
    }

    /// Fetches an off-chain metadata document. `Ok(None)` means the host
    /// answered definitively that it is not there — the dead-host case, which
    /// costs exactly one request because 404 is terminal.
    pub async fn fetch_document(&self, url: &str) -> Result<Option<Value>, DasError> {
        let mut attempt = 1;
        loop {
            let error = match self.http.get(url).send().await {
                Ok(response) => {
                    let status = response.status();
                    if status.is_success() {
                        return match response.json::<Value>().await {
                            Ok(value) => Ok(Some(value)),
                            // Valid HTTP, invalid JSON: a placeholder page,
                            // not a transient fault. Retrying cannot help.
                            Err(e) => {
                                log::debug!("document {url} was not JSON: {e}");
                                Ok(None)
                            }
                        };
                    }
                    DasError::Status {
                        method: format!("GET {url}"),
                        status,
                    }
                }
                Err(e) => DasError::Transport(e.to_string()),
            };

            if !is_retryable(&error) {
                return Ok(None);
            }
            if attempt >= self.max_attempts {
                return Err(error);
            }
            tokio::time::sleep(backoff(attempt)).await;
            attempt += 1;
        }
    }

    /// Probes an image URL. `HEAD` first, falling back to a one-byte ranged
    /// `GET` for hosts that reject `HEAD` (common on IPFS/Arweave gateways).
    pub async fn probe_image(&self, url: &str) -> Reachability {
        match self.http.head(url).send().await {
            Ok(response) if response.status().is_success() => return Reachability::Ok,
            Ok(response) if head_unsupported(response.status()) => {}
            Ok(response) if is_gone(response.status()) => return Reachability::Dead,
            Ok(_) => return Reachability::Undetermined,
            // A URL the client cannot even build a request for (ipfs://,
            // ar://, or plain garbage) is dead for our purposes.
            Err(e) if e.is_builder() || e.is_request() && e.url().is_none() => {
                return Reachability::Dead
            }
            Err(_) => return Reachability::Undetermined,
        }

        match self
            .http
            .get(url)
            .header(reqwest::header::RANGE, "bytes=0-0")
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => Reachability::Ok,
            Ok(response) if is_gone(response.status()) => Reachability::Dead,
            _ => Reachability::Undetermined,
        }
    }
}

#[derive(Deserialize)]
struct RpcResponse {
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    #[serde(default)]
    message: String,
}

/// Transport faults and server-side congestion are worth another try;
/// everything else is the server's considered answer. 404 in particular must
/// be terminal — it is what a dead metadata host returns for every one of a
/// collection's assets, and retrying would multiply that by `max_attempts`.
fn is_retryable(error: &DasError) -> bool {
    match error {
        DasError::Transport(_) => true,
        DasError::Status { status, .. } => {
            status.is_server_error()
                || *status == StatusCode::TOO_MANY_REQUESTS
                || *status == StatusCode::REQUEST_TIMEOUT
        }
        DasError::Rpc { .. } | DasError::Decode { .. } | DasError::BatchTooLarge(_) => false,
    }
}

fn head_unsupported(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::METHOD_NOT_ALLOWED | StatusCode::NOT_IMPLEMENTED | StatusCode::BAD_REQUEST
    )
}

fn is_gone(status: StatusCode) -> bool {
    matches!(status, StatusCode::NOT_FOUND | StatusCode::GONE)
}

fn backoff(attempt: u32) -> Duration {
    Duration::from_millis(250u64.saturating_mul(1 << attempt.min(5)))
}

/// Strips the API key so a URL can be logged. The key is a secret and this
/// crate logs request failures by default.
pub fn redact(url: &str) -> String {
    match url.find("api-key=") {
        Some(index) => format!("{}api-key=***", &url[..index]),
        None => url.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_api_key_never_reaches_a_log_line() {
        assert_eq!(
            redact("https://mainnet.helius-rpc.com/?api-key=secret-value"),
            "https://mainnet.helius-rpc.com/?api-key=***"
        );
        assert_eq!(
            redact("https://example.invalid/1.json"),
            "https://example.invalid/1.json"
        );
    }

    #[test]
    fn only_transient_failures_are_retried() {
        let status = |code: u16| DasError::Status {
            method: "getAssetBatch".into(),
            status: StatusCode::from_u16(code).unwrap(),
        };
        assert!(is_retryable(&DasError::Transport("reset".into())));
        assert!(is_retryable(&status(429)));
        assert!(is_retryable(&status(503)));
        // The dead-host case: one request per asset, never four.
        assert!(!is_retryable(&status(404)));
        assert!(!is_retryable(&status(403)));
        assert!(!is_retryable(&DasError::Rpc {
            method: "getSlot".into(),
            message: "bad".into(),
        }));
    }

    #[test]
    fn backoff_grows_and_stays_bounded() {
        assert!(backoff(1) < backoff(2));
        assert!(backoff(2) < backoff(3));
        assert!(backoff(20) <= Duration::from_secs(8));
    }

    /// Pins the camelCase rename: without it `block_time` deserializes to
    /// `None` on every row and the recovery path silently loses its timestamps.
    #[test]
    fn signature_info_reads_the_rpc_shape() {
        let info: SignatureInfo = serde_json::from_value(serde_json::json!({
            "signature": "SYNsig",
            "slot": 443_800_000_i64,
            "blockTime": 1_700_000_000_i64,
            "err": null,
            "memo": null,
            "confirmationStatus": "confirmed",
        }))
        .unwrap();
        assert_eq!(info.slot, 443_800_000);
        assert_eq!(info.block_time, Some(1_700_000_000));
        assert!(info.block_time_utc().is_some());
        assert!(!info.failed());

        let failed: SignatureInfo = serde_json::from_value(serde_json::json!({
            "signature": "SYNsig", "slot": 1, "err": {"InstructionError": [0, "Custom"]},
        }))
        .unwrap();
        assert!(failed.failed());
        assert_eq!(failed.block_time_utc(), None);
    }

    #[tokio::test]
    async fn oversized_batches_are_rejected_before_the_network() {
        let client = DasClient::with_endpoint("https://example.invalid", "k").unwrap();
        let ids = vec![String::new(); MAX_BATCH + 1];
        let error = client.get_asset_batch(&ids).await.unwrap_err();
        assert!(matches!(error, DasError::BatchTooLarge(n) if n == MAX_BATCH + 1));
    }
}
