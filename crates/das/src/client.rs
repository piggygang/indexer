//! Helius DAS over JSON-RPC, plus the plain HTTPS fetches for off-chain
//! metadata and image reachability.
//!
//! Hand-rolled rather than the `helius` SDK: DAS is a handful of JSON-RPC
//! POSTs, while the SDK hard-depends on the whole `solana-sdk`/`solana-client`
//! tree and defaults to `native-tls`, against this repo's rustls-only policy.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use reqwest::redirect::Policy;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::asset::Asset;

/// DAS caps `getAssetBatch` at 1 000 ids.
pub const MAX_BATCH: usize = 1000;

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

impl DasError {
    /// Was this a rate limit or a spent plan?
    ///
    /// Distinct from "retryable" on purpose. Retrying a 429 is what makes a
    /// budget problem worse, so [`is_retryable`] says no — but a caller that
    /// can *wait* rather than fail needs to tell "slow down" apart from
    /// "definitively absent", and the two are the same `Err` otherwise. The
    /// webhook drain defers a row instead of burning one of its attempts on
    /// it; `fetch_document` returns the error instead of reporting a document
    /// that exists as missing.
    pub fn is_rate_limited(&self) -> bool {
        matches!(
            self,
            DasError::Status { status, .. } if *status == StatusCode::TOO_MANY_REQUESTS
        )
    }
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

/// One transaction from [`DasClient::get_transactions_for_address`].
///
/// `value` is the untouched `getTransaction`-shaped object, which is what
/// lets the archival crawl feed the *same* decoder as the live path.
#[derive(Debug, Clone)]
pub struct ArchivedTx {
    pub signature: String,
    pub slot: i64,
    /// Carried by the archival response itself, so the crawl never needs a
    /// separate `getBlockTime`.
    pub block_time: Option<DateTime<Utc>>,
    pub value: Value,
}

/// One page of [`DasClient::get_transactions_for_address`].
///
/// Note the termination rule: Helius returns `pagination_token` **even on the
/// last non-empty page**, so a caller that stops when the token is `None`
/// stops one page early on some addresses and never at all on others. Page
/// until `data` is empty.
#[derive(Debug, Default)]
pub struct TxPage {
    pub data: Vec<ArchivedTx>,
    pub pagination_token: Option<String>,
}

/// A minimum spacing between outbound RPC calls.
///
/// Hand-rolled for the same reason the retry loop is: one `Instant` and a
/// mutex held only long enough to claim a slot, against a `governor`/`tower`
/// dependency the workspace does not otherwise need. The lock is never held
/// across the sleep, so N concurrent callers queue rather than serialize.
#[derive(Debug)]
struct RateLimiter {
    interval: Duration,
    next: tokio::sync::Mutex<tokio::time::Instant>,
}

impl RateLimiter {
    fn new(per_second: u32) -> Self {
        Self {
            interval: Duration::from_secs_f64(1.0 / f64::from(per_second.max(1))),
            next: tokio::sync::Mutex::new(tokio::time::Instant::now()),
        }
    }

    async fn acquire(&self) {
        let deadline = {
            let mut next = self.next.lock().await;
            let now = tokio::time::Instant::now();
            let deadline = (*next).max(now);
            *next = deadline + self.interval;
            deadline
        };
        tokio::time::sleep_until(deadline).await;
    }
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

/// How a `searchAssets` call selects its population.
///
/// One variant per membership rule the registry can express, so the caller
/// never has to know which JSON field a collection's identity lives in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchFilter {
    /// Metaplex Core, and Token Metadata with a certified collection.
    Collection(String),
    /// Token Metadata without one: the verified creator is the only
    /// collection-wide handle that exists.
    Creator(String),
}

/// How a `searchAssets` page is ordered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SearchOrder {
    /// Stable and total — the only safe order to paginate an enumeration with.
    IdAsc,
    /// Newest on-chain action first. Not stable under concurrent activity, so
    /// it is only ever read as a single page, never paged through.
    RecentActionDesc,
}

impl SearchOrder {
    fn as_json(self) -> Value {
        match self {
            Self::IdAsc => json!({"sortBy": "id", "sortDirection": "asc"}),
            Self::RecentActionDesc => json!({"sortBy": "recent_action", "sortDirection": "desc"}),
        }
    }
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
    /// RPC and DAS: the requests that carry the key. Never follows a
    /// redirect — see [`http_client`].
    http: Client,
    /// Metadata documents and image probes: public URLs the chain points at,
    /// on hosts that redirect as a matter of course. Carries no credential,
    /// and is never handed the endpoint.
    web: Client,
    endpoint: String,
    api_key: String,
    max_attempts: u32,
    /// `None` = unthrottled, which is right for ALG-621's ~18 calls. The
    /// per-asset archival crawl sets it.
    limiter: Option<Arc<RateLimiter>>,
}

impl DasClient {
    /// The only constructor, and it takes the endpoint: this crate knows no
    /// host of its own. Which cluster a client talks to is the caller's
    /// configuration (`SOLANA_RPC_URL`), passed as a `&str` exactly as the key
    /// is — a hardcoded default here is how a devnet deployment would have
    /// kept reading mainnet without anyone having chosen that.
    ///
    /// `api_key` is appended as `?api-key=`; empty means "send none", which is
    /// what the integration tests' loopback fake Helius wants.
    pub fn with_endpoint(endpoint: &str, api_key: &str) -> Result<Self, DasError> {
        Ok(Self {
            http: http_client(Duration::from_secs(30), Redirects::Refuse)?,
            web: http_client(Duration::from_secs(30), Redirects::Follow)?,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            api_key: api_key.to_string(),
            max_attempts: 4,
            limiter: None,
        })
    }

    pub fn with_max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = attempts.max(1);
        self
    }

    /// Caps outbound RPC at `per_second` calls. Shared across clones, so
    /// handing the same client to N concurrent crawl tasks throttles the
    /// fleet rather than each task.
    ///
    /// Retries pass through the limiter too: a 429 that is retried
    /// immediately is what earns the next one.
    pub fn with_rate_limit(mut self, per_second: u32) -> Self {
        self.limiter = Some(Arc::new(RateLimiter::new(per_second)));
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

    /// The genesis hash of the cluster this endpoint serves.
    ///
    /// The cluster's identity as the node itself states it — the one thing an
    /// endpoint cannot be wrong about by accident. See [`crate::cluster`].
    pub async fn get_genesis_hash(&self) -> Result<String, DasError> {
        let result = self.rpc("getGenesisHash", json!([])).await?;
        result
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| DasError::Rpc {
                method: "getGenesisHash".into(),
                message: format!("expected a string, got {result}"),
            })
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

    /// Full transaction history for an address, oldest first, in one call.
    ///
    /// Helius-only (`getTransactionsForAddress`), and the reason the archival
    /// crawl is affordable: it replaces `getSignaturesForAddress` plus one
    /// `getTransaction` per signature with a single paged request, and the
    /// entries it returns are the same `getTransaction` shape the decoder
    /// already accepts. `blockTime` rides along, so no `getBlockTime` either.
    ///
    /// Page until [`TxPage::data`] is empty — see the note on [`TxPage`].
    pub async fn get_transactions_for_address(
        &self,
        address: &str,
        pagination_token: Option<&str>,
        limit: u32,
    ) -> Result<TxPage, DasError> {
        let mut options = json!({
            "transactionDetails": "full",
            // Oldest first: the crawl derives an ownership chain, and
            // `activity::record` applies an event only when it is at or after
            // the asset's frontier. Walking backwards would flag every asset
            // dirty and hand the whole collection to `rebuild-ownership`.
            "sortOrder": "asc",
            "limit": limit,
            "commitment": "finalized",
            "encoding": "jsonParsed",
            "maxSupportedTransactionVersion": 0,
        });
        if let Some(token) = pagination_token {
            options["paginationToken"] = json!(token);
        }
        let value = self
            .rpc("getTransactionsForAddress", json!([address, options]))
            .await?;

        let pagination_token = value
            .get("paginationToken")
            .and_then(Value::as_str)
            .map(str::to_string);
        let data = value
            .get("data")
            .and_then(Value::as_array)
            .map(|rows| rows.iter().filter_map(archived_tx).collect())
            .unwrap_or_default();
        Ok(TxPage {
            data,
            pagination_token,
        })
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

    /// One page of a collection's assets, oldest id first. Used for both
    /// `core_collection` and `tm_collection` — the only difference between
    /// them is which address the registry supplies.
    pub async fn search_assets(
        &self,
        collection: &str,
        page: u32,
        limit: u32,
        grand_total: bool,
    ) -> Result<SearchPage, DasError> {
        self.search(
            &SearchFilter::Collection(collection.to_string()),
            SearchOrder::IdAsc,
            page,
            limit,
            grand_total,
        )
        .await
    }

    /// The newest-acted-on assets a filter matches, most recent first.
    ///
    /// This is the reconciliation tip probe: instead of re-reading every
    /// tracked asset to find the handful that moved, ask DAS which ones acted
    /// most recently and read down until the answers stop being news. One call
    /// per filter, against 18 `getAssetBatch` calls for a full sweep.
    pub async fn search_recent(
        &self,
        filter: &SearchFilter,
        limit: u32,
    ) -> Result<SearchPage, DasError> {
        self.search(filter, SearchOrder::RecentActionDesc, 1, limit, false)
            .await
    }

    async fn search(
        &self,
        filter: &SearchFilter,
        order: SearchOrder,
        page: u32,
        limit: u32,
        grand_total: bool,
    ) -> Result<SearchPage, DasError> {
        let mut params = json!({
            "page": page,
            "limit": limit,
            "sortBy": order.as_json(),
            "options": {"showUnverifiedCollections": true, "showGrandTotal": grand_total},
        });
        match filter {
            SearchFilter::Collection(address) => {
                params["grouping"] = json!(["collection", address]);
            }
            SearchFilter::Creator(address) => {
                // The verified creator is what identifies a Token Metadata
                // collection that has no certified collection — the same
                // signal `collections.verified_creator` holds and the
                // allowlist rule validates against.
                params["creatorAddress"] = json!(address);
                params["creatorVerified"] = json!(true);
            }
        }
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
            if let Some(limiter) = &self.limiter {
                limiter.acquire().await;
            }
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
                                    // The server's words, and these errors are
                                    // logged and stored: see [`mask`].
                                    message: mask(&error.message, &[&self.api_key]),
                                });
                            }
                            Ok(RpcResponse { result, .. }) => {
                                return Ok(result.unwrap_or(Value::Null))
                            }
                            Err(e) => Err(transport(e)),
                        }
                    } else {
                        Err(DasError::Status {
                            method: method.into(),
                            status,
                        })
                    }
                }
                Err(e) => Err(transport(e)),
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
            let error = match self.web.get(url).send().await {
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

            // `Ok(None)` is load-bearing: it means "the host answered
            // definitively that this is not there", which reaches
            // `upsert_batch` as `attributes: Some(vec![])` and DELETES the
            // asset's stored attributes. A throttled gateway has answered
            // nothing of the kind, so it must surface as an error.
            if error.is_rate_limited() {
                return Err(error);
            }
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
        match self.web.head(url).send().await {
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
            .web
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
/// One `getTransactionsForAddress` row.
///
/// The signature lives at `transaction.signatures[0]` rather than a top-level
/// field — the row is a `getTransaction` result with `slot`/`blockTime`
/// alongside, not an enhanced-transaction object. A row without a signature or
/// a slot is dropped rather than guessed at: it could not be keyed in
/// `activity` (`UNIQUE (asset_id, signature, seq)`) anyway.
fn archived_tx(row: &Value) -> Option<ArchivedTx> {
    let signature = row
        .pointer("/transaction/signatures/0")
        .and_then(Value::as_str)?
        .to_string();
    let slot = row.get("slot").and_then(Value::as_i64)?;
    let block_time = row
        .get("blockTime")
        .and_then(Value::as_i64)
        .and_then(|secs| Utc.timestamp_opt(secs, 0).single());
    Some(ArchivedTx {
        signature,
        slot,
        block_time,
        value: row.clone(),
    })
}

/// Retryable means "the same request might succeed next time, and asking again
/// is worth what it costs".
///
/// **429 is deliberately absent.** Helius bills rejected requests, so retrying
/// a rate-limit answer multiplied every call by `max_attempts` at exactly the
/// moment the budget was the constraint — and an exhausted plan answers `429`
/// to everything, so a retrying client keeps spending after the credits are
/// gone. Callers that can do something better than fail — wait, defer a queued
/// row — ask [`DasError::is_rate_limited`] instead.
fn is_retryable(error: &DasError) -> bool {
    match error {
        DasError::Transport(_) => true,
        DasError::Status { status, .. } => {
            status.is_server_error() || *status == StatusCode::REQUEST_TIMEOUT
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

/// What a client built by [`http_client`] does when it is told to go elsewhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Redirects {
    /// For every request that carries a credential: RPC, DAS and the webhook
    /// management API. The endpoint is configuration — explicit, never
    /// derived — and a `Location` header is a host nobody wrote down. Following
    /// one would repeat the request there: the key if the redirect kept the
    /// query, in the clear if it also dropped to `http://`, and for a
    /// `307`/`308` the body too, which on `create`/`update` is the webhook
    /// secret. So the redirect is returned as the answer it is — an HTTP 3xx
    /// error naming the method — and fixing the endpoint is the operator's.
    Refuse,
    /// For public documents the chain points at: metadata and images, on
    /// gateways that redirect as a matter of course. reqwest's own default.
    Follow,
}

/// The one place an HTTP client is built in this workspace.
///
/// **No `Referer`, for either kind.** reqwest tells the next host where it
/// came from when it follows a redirect: the previous URL, path and query
/// included. A credentialed client no longer follows one at all, so for it
/// this is the second lock on the same door; for the metadata client the
/// previous URL is public, and announcing it is merely pointless.
pub(crate) fn http_client(timeout: Duration, redirects: Redirects) -> Result<Client, DasError> {
    Client::builder()
        .timeout(timeout)
        .connect_timeout(Duration::from_secs(10))
        .user_agent(concat!("indexer-das/", env!("CARGO_PKG_VERSION")))
        .redirect(match redirects {
            Redirects::Refuse => Policy::none(),
            Redirects::Follow => Policy::limited(10),
        })
        .referer(false)
        .build()
        .map_err(transport)
}

/// A transport failure, with the request URL removed and the cause kept.
///
/// reqwest's `Display` is "error sending request for url (…)": the whole URL,
/// query string included, and the query string is where `api-key=` travels.
/// Every caller logs these errors — the retry loop at `debug`, the ingester's
/// boot check at `warn`, the admin CLI straight to the terminal — so the URL
/// has to go *here*, at the one place a `reqwest::Error` becomes a string,
/// rather than at each of them. That it was not is how a single refused
/// connection used to put the key in the logs.
///
/// Nothing useful is lost, and something is gained: the method is in the
/// surrounding message, the endpoint is logged alongside (see [`redact`]), and
/// reqwest's own text never said *why* — "error sending request" is the same
/// for a refused connection, a DNS failure and a TLS fault. The cause chain is
/// appended instead. [`scrub`] then masks any `api-key=` that a cause might
/// still spell out; none does today, and a secret is not where to rely on
/// that staying true.
///
/// For the RPC and webhook-management paths only — the two whose URLs carry
/// the key. `fetch_document` keeps reqwest's message, because there the URL is
/// a public metadata document and naming it is the whole diagnostic.
pub(crate) fn transport(error: reqwest::Error) -> DasError {
    let error = error.without_url();
    let mut message = error.to_string();
    let mut cause = std::error::Error::source(&error);
    while let Some(inner) = cause {
        message.push_str(": ");
        message.push_str(&inner.to_string());
        cause = inner.source();
    }
    DasError::Transport(scrub(&message))
}

/// Masks the value of every `api-key=` in free text, leaving the rest intact.
///
/// Unlike [`redact`], which takes a URL and can simply keep its origin, this
/// takes a sentence: the text around the key matters too.
pub(crate) fn scrub(message: &str) -> String {
    const MARKER: &str = "api-key=";
    let mut out = String::with_capacity(message.len());
    let mut rest = message;
    while let Some(index) = rest.find(MARKER) {
        let (before, after) = rest.split_at(index + MARKER.len());
        out.push_str(before);
        out.push_str("***");
        // The value runs to the end of the query parameter, or of the URL.
        let end = after
            .find(|c: char| c == '&' || c == ')' || c == '"' || c == '\'' || c.is_whitespace())
            .unwrap_or(after.len());
        rest = &after[end..];
    }
    out.push_str(rest);
    out
}

/// Anything shorter is not a secret worth mangling a message for — and a
/// one-letter test key must not turn every `k` in an error into `***`.
const MIN_SECRET_LEN: usize = 8;

/// Masks secrets in text that came from the other side of a connection.
///
/// [`scrub`] handles the shape *we* send (`api-key=…`). This also removes, by
/// value, the secrets the calling client itself holds: an error page or a
/// validation error that echoes the request back echoes them in whatever shape
/// the server likes — a bare key in a sentence, `"authHeader":"…"` in JSON —
/// and these messages are logged and stored like any other.
pub(crate) fn mask(text: &str, secrets: &[&str]) -> String {
    let mut out = scrub(text);
    for secret in secrets
        .iter()
        .filter(|secret| secret.len() >= MIN_SECRET_LEN)
    {
        out = out.replace(secret, "***");
    }
    out
}

/// Reduces a URL to its origin — `scheme://host[:port]` — so it can appear in
/// a log line.
///
/// Everything after the origin can be a credential: the query carries
/// `api-key=`, a dedicated node's access token is usually the path, and
/// userinfo is a password by definition. Masking only the query was enough
/// while the host was a constant in this crate; it stopped being enough when
/// the endpoint became configuration, because the config crate accepts a path
/// and hides it from the boot log for exactly this reason.
///
/// Text that is not a URL at all says nothing about itself: there is no part
/// of an unparseable value that is known to be safe.
pub fn redact(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return "<redacted>".to_string();
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let host = authority.rsplit('@').next().unwrap_or_default();
    format!("{scheme}://{host}")
}

/// Loopback fakes shared by this crate's unit tests.
#[cfg(test)]
pub(crate) mod test_support {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::oneshot;

    /// Reads one whole HTTP request — head, and body if it declares one.
    /// Answering before the body has been read can reset the connection under
    /// the client.
    async fn read_request(stream: &mut TcpStream) -> String {
        let mut buffer = Vec::new();
        let mut chunk = [0u8; 4096];
        let head_end = loop {
            if let Some(index) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
                break index + 4;
            }
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => break buffer.len(),
                Ok(read) => buffer.extend_from_slice(&chunk[..read]),
            }
        };
        let head = String::from_utf8_lossy(&buffer[..head_end]).into_owned();
        let declared = head
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())?
            })
            .unwrap_or(0);
        while buffer.len() < head_end + declared {
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(read) => buffer.extend_from_slice(&chunk[..read]),
            }
        }
        String::from_utf8_lossy(&buffer).into_owned()
    }

    async fn respond(stream: &mut TcpStream, status: &str, extra_headers: &str, body: &str) {
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\n{extra_headers}\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.flush().await;
    }

    /// A loopback server that answers its one request with `status` and a body
    /// built from the request it received — `reply(request)` — the way an
    /// error page or a validation error repeats what it was sent.
    pub(crate) async fn answering_with(
        status: &'static str,
        reply: impl FnOnce(&str) -> String + Send + 'static,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let request = read_request(&mut stream).await;
                respond(&mut stream, status, "", &reply(&request)).await;
            }
        });
        endpoint
    }

    /// Two loopback servers. The first — returned as the endpoint — answers
    /// its one request with a `307` to the second; the second hands back the
    /// whole request it received and then answers with `body`. That request is
    /// everything a redirect target learns about the client.
    ///
    /// It is handed over *before* the answer goes out, so by the time a client
    /// call has returned, `try_recv` on the receiver is a definite statement:
    /// a value means the redirect was followed, empty means the second origin
    /// never heard from us.
    pub(crate) async fn redirect_to_another_origin(
        body: &'static str,
    ) -> (String, oneshot::Receiver<String>) {
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let location = format!("http://{}/elsewhere", target.local_addr().unwrap());
        let (seen_tx, seen_rx) = oneshot::channel();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = target.accept().await {
                let request = read_request(&mut stream).await;
                let _ = seen_tx.send(request);
                respond(&mut stream, "200 OK", "", body).await;
            }
        });

        let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", origin.local_addr().unwrap());
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = origin.accept().await {
                read_request(&mut stream).await;
                let header = format!("Location: {location}\r\n");
                respond(&mut stream, "307 Temporary Redirect", &header, "").await;
            }
        });
        (endpoint, seen_rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_logged_url_is_its_origin_and_nothing_else() {
        assert_eq!(
            redact("https://mainnet.helius-rpc.com/?api-key=secret-value"),
            "https://mainnet.helius-rpc.com"
        );
        // A dedicated node keeps its token in the path, and the config crate
        // accepts one — so the path goes too, not just the query.
        assert_eq!(
            redact("https://rpc.example.test/t0k3n/?api-key=secret-value"),
            "https://rpc.example.test"
        );
        assert_eq!(
            redact("https://user:t0k3n@rpc.example.test:8443/x#frag"),
            "https://rpc.example.test:8443"
        );
        assert_eq!(redact("http://[::1]:8899/"), "http://[::1]:8899");
        // Not a URL: no part of it is known to be safe to print.
        assert_eq!(redact("t0k3n:x@rpc.example.test"), "<redacted>");
    }

    #[test]
    fn scrubbing_masks_the_key_and_keeps_the_sentence() {
        assert_eq!(
            scrub("error for url (https://rpc.example.test/?api-key=s3cr3t): refused"),
            "error for url (https://rpc.example.test/?api-key=***): refused"
        );
        // Every occurrence, and only the value: a following parameter stays.
        assert_eq!(
            scrub("a ?api-key=one&commitment=confirmed then b ?api-key=two"),
            "a ?api-key=***&commitment=confirmed then b ?api-key=***"
        );
        assert_eq!(scrub("connection refused"), "connection refused");
        assert_eq!(scrub(""), "");
    }

    #[test]
    fn only_transient_failures_are_retried() {
        let status = |code: u16| DasError::Status {
            method: "getAssetBatch".into(),
            status: StatusCode::from_u16(code).unwrap(),
        };
        assert!(is_retryable(&DasError::Transport("reset".into())));
        assert!(is_retryable(&status(503)));
        // Helius bills rejected requests, so a retried 429 spends four credits
        // to learn the same thing once. Callers branch on `is_rate_limited`.
        assert!(!is_retryable(&status(429)));
        assert!(status(429).is_rate_limited());
        assert!(!status(503).is_rate_limited());
        assert!(!DasError::Transport("reset".into()).is_rate_limited());
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

    /// reqwest's `Display` ends " for url (…)", query string and all, and the
    /// query string is where the key travels. These errors are logged by every
    /// caller — at `warn` by the ingester's boot check, on the terminal by the
    /// admin CLI — so one transport fault used to be enough to put the key in
    /// Railway's logs.
    #[tokio::test]
    async fn a_transport_error_never_carries_the_api_key() {
        // Nothing listens on port 1, so the connect is refused at once.
        let client = DasClient::with_endpoint("http://127.0.0.1:1", "s3cr3t-key")
            .unwrap()
            .with_max_attempts(1);
        let error = client.get_slot().await.unwrap_err();
        assert!(matches!(error, DasError::Transport(_)), "{error:?}");
        let rendered = format!("{error} / {error:?}");
        assert!(!rendered.contains("s3cr3t-key"), "leaked: {rendered}");
        // The cause survives, which the bare reqwest message never carried:
        // "error sending request" alone says nothing about why.
        assert!(
            rendered.to_lowercase().contains("connect"),
            "the cause should be named: {rendered}"
        );
    }

    /// A request that carries the key never follows a redirect. The endpoint
    /// is configuration — explicit, never derived — and a `Location` header is
    /// a host nobody wrote down: following it would send the request, and with
    /// reqwest's defaults a `Referer` naming the URL the key is in, wherever a
    /// mistyped host, a parked domain or a proxy chose to point.
    #[tokio::test]
    async fn an_rpc_redirect_is_refused_and_the_next_host_hears_nothing() {
        let (endpoint, mut seen) = test_support::redirect_to_another_origin(
            r#"{"jsonrpc":"2.0","id":"indexer","result":123}"#,
        )
        .await;
        let client = DasClient::with_endpoint(&endpoint, "s3cr3t-key").unwrap();

        // The redirect itself is the answer, and the operator's to act on.
        let error = client.get_slot().await.unwrap_err();
        assert!(
            matches!(&error, DasError::Status { status, .. }
                if *status == StatusCode::TEMPORARY_REDIRECT),
            "unexpected error: {error:?}"
        );
        // Asking again would get the same `Location`, so it is not retried —
        // which `with_endpoint`'s four attempts would otherwise have shown.
        assert!(!is_retryable(&error));
        assert!(!format!("{error} {error:?}").contains("s3cr3t-key"));
        // No request reached the second origin: no key, no `Referer`, no body.
        assert!(seen.try_recv().is_err(), "the redirect was followed");
    }

    /// A JSON-RPC error is the server's own sentence, and it ends up in logs
    /// and in `last_error` columns. A node or a proxy that quotes the key back
    /// — bare, not as `api-key=` — must not get it stored that way.
    #[tokio::test]
    async fn an_rpc_error_that_quotes_the_key_back_is_masked() {
        let endpoint = test_support::answering_with("200 OK", |_request| {
            json!({"jsonrpc": "2.0", "id": "indexer",
                   "error": {"code": -32401, "message": "key s3cr3t-key-value is not allowed"}})
            .to_string()
        })
        .await;
        let client = DasClient::with_endpoint(&endpoint, "s3cr3t-key-value").unwrap();

        let error = client.get_slot().await.unwrap_err().to_string();
        assert!(
            error.contains("is not allowed"),
            "the message survives: {error}"
        );
        assert!(!error.contains("s3cr3t"), "leaked: {error}");
    }

    #[test]
    fn masking_removes_held_secrets_by_value_and_leaves_short_ones_alone() {
        assert_eq!(
            mask(
                r#"bad request: {"authHeader":"wh-s3cr3t-value"} at /v0/webhooks?api-key=k3y-k3y-k3y"#,
                &["k3y-k3y-k3y", "wh-s3cr3t-value"],
            ),
            r#"bad request: {"authHeader":"***"} at /v0/webhooks?api-key=***"#
        );
        // A one-letter test key is not a secret, and masking it would turn
        // every `k` in the message into `***`.
        assert_eq!(mask("block not known", &["k", ""]), "block not known");
    }

    /// Metadata is the other kind of request: a public document the chain
    /// points at, fetched from hosts that redirect as a matter of course
    /// (gateways do), and carrying nothing of ours. It still follows — and it
    /// still does not announce where it came from.
    #[tokio::test]
    async fn a_metadata_fetch_still_follows_redirects_and_carries_no_credential() {
        let (host, seen) = test_support::redirect_to_another_origin(r##"{"name":"#1"}"##).await;
        // Its RPC endpoint is irrelevant here; the key must not travel anyway.
        let client = DasClient::with_endpoint("http://127.0.0.1:1", "s3cr3t-key").unwrap();

        let document = client
            .fetch_document(&format!("{host}/1.json"))
            .await
            .unwrap();
        assert_eq!(document, Some(json!({"name": "#1"})));

        let request = seen.await.unwrap();
        assert!(
            !request.to_lowercase().contains("referer:"),
            "a Referer was sent to the redirect target:\n{request}"
        );
        assert!(!request.contains("s3cr3t-key"), "leaked:\n{request}");
    }

    #[tokio::test]
    async fn oversized_batches_are_rejected_before_the_network() {
        let client = DasClient::with_endpoint("https://example.invalid", "k").unwrap();
        let ids = vec![String::new(); MAX_BATCH + 1];
        let error = client.get_asset_batch(&ids).await.unwrap_err();
        assert!(matches!(error, DasError::BatchTooLarge(n) if n == MAX_BATCH + 1));
    }
}
