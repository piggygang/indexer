//! Is this endpoint on the cluster the configuration says it is?
//!
//! `SOLANA_CLUSTER` and `SOLANA_RPC_URL` are independent variables — no
//! endpoint is ever derived from the cluster — so they can disagree, and
//! nothing downstream would notice. DAS on the wrong chain does not fail: it
//! answers `searchAssets` for a collection it has never heard of with an empty
//! page, and the reconciliation sweep reads an empty page for a
//! `core_collection` as *every stored asset left the collection*, flipping the
//! whole population to `removed`. A healthy-looking process, an empty API.
//!
//! The config crate's offline check catches a hostname that *names* another
//! cluster. This is the other half, and the only one that covers a host with
//! no cluster in its name (a dedicated node, a proxy): ask the node.
//! `getGenesisHash` is the cluster's identity as the node itself states it,
//! costs one credit, and cannot be wrong by accident.
//!
//! The expected hash arrives as a `&str` for the same reason the API key and
//! the endpoint do: this crate does not depend on `indexer-config`.

use crate::client::{DasClient, DasError};

#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    /// The endpoint answered, and it is another chain. No restart fixes this;
    /// only the configuration does.
    #[error(
        "the RPC endpoint is on another cluster: its genesis hash is {actual}, \
         and the configured cluster's is {expected}"
    )]
    Mismatch { expected: String, actual: String },
    /// The endpoint did not answer the question, which says nothing about
    /// which cluster it is on. Kept apart from [`ClusterError::Mismatch`]
    /// because the two want opposite handling: a one-shot command gives up on
    /// either, while a long-running service must wait this one out rather than
    /// exit — and must never treat it as a pass.
    #[error("could not read the RPC endpoint's genesis hash: {0}")]
    Unverified(#[source] DasError),
}

/// Compares the endpoint's genesis hash with the one the configured cluster
/// must have.
pub async fn verify(das: &DasClient, expected_genesis_hash: &str) -> Result<(), ClusterError> {
    let actual = das
        .get_genesis_hash()
        .await
        .map_err(ClusterError::Unverified)?;
    if actual == expected_genesis_hash {
        Ok(())
    } else {
        Err(ClusterError::Mismatch {
            expected: expected_genesis_hash.to_string(),
            actual,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    // Synthetic: the check compares strings, so two that differ are all it
    // needs. The real hashes live in `indexer-config`, next to the cluster.
    const OURS: &str = "SYNgenesisOURS1111111111111111111111111111";
    const THEIRS: &str = "SYNgenesisTHEIRS11111111111111111111111111";

    /// A loopback node that answers `getGenesisHash` with `body`.
    async fn node(body: serde_json::Value) -> DasClient {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let payload = body.to_string();
                tokio::spawn(async move {
                    let mut chunk = [0u8; 4096];
                    let _ = stream.read(&mut chunk).await;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                         Content-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                        payload.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.flush().await;
                });
            }
        });
        DasClient::with_endpoint(&endpoint, "").unwrap()
    }

    fn says(hash: &str) -> serde_json::Value {
        serde_json::json!({"jsonrpc": "2.0", "id": "indexer", "result": hash})
    }

    /// Three answers, three different outcomes — and the third is the one that
    /// must never be folded into either of the others: an endpoint that did
    /// not say which chain it is on has neither passed nor failed.
    #[tokio::test]
    async fn the_three_answers_stay_three_outcomes() {
        assert!(verify(&node(says(OURS)).await, OURS).await.is_ok());

        let error = verify(&node(says(THEIRS)).await, OURS).await.unwrap_err();
        assert!(
            matches!(&error, ClusterError::Mismatch { expected, actual }
                if expected == OURS && actual == THEIRS),
            "unexpected error: {error}"
        );

        // A node that is up but cannot answer, and one that answers nonsense.
        for body in [
            serde_json::json!({"jsonrpc": "2.0", "id": "indexer",
                               "error": {"code": -32005, "message": "Node is unhealthy"}}),
            serde_json::json!({"jsonrpc": "2.0", "id": "indexer", "result": 42}),
        ] {
            let error = verify(&node(body).await, OURS).await.unwrap_err();
            assert!(
                matches!(error, ClusterError::Unverified(_)),
                "unexpected error: {error}"
            );
        }
    }
}
