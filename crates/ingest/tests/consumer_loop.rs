//! The scaffold proof for ALG-618: a consumer written purely against
//! `Arc<dyn IngestSource>` survives a crash/restart with no gap and no
//! double-count, and observes live subscription updates — all without any
//! transport dependency.

use std::collections::HashSet;
use std::sync::Arc;

use futures_util::StreamExt;
use indexer_ingest::{
    IngestEvent, IngestSource, MockSource, ResumeFrom, StreamStatus, SubscriptionSpec,
    TransactionFilter,
};
use tokio::sync::watch;

fn scripted_source() -> Arc<dyn IngestSource> {
    Arc::new(MockSource::new(vec![
        IngestEvent::Status(StreamStatus::Connected),
        MockSource::tx("sig-a", 10, "tm-txs"),
        MockSource::account("token-acct-1", 10, "tm-accounts"),
        MockSource::checkpoint(10),
        MockSource::tx("sig-b", 11, "tm-txs"),
        MockSource::checkpoint(11),
        MockSource::tx("sig-c", 12, "tm-txs"),
        // "Crash" here: slot 12 work was seen but never checkpointed.
    ]))
}

#[tokio::test]
async fn consumer_resumes_from_checkpoint_without_gap_or_double_count() {
    let source = scripted_source();
    let (_spec_tx, spec_rx) = watch::channel(SubscriptionSpec::default());

    // First run: `seen` stands in for signature-keyed idempotent upserts,
    // `cursor` for ingest_state.last_processed_slot (persisted only on
    // SlotCheckpoint — never mid-slot).
    let mut seen: HashSet<String> = HashSet::new();
    let mut cursor: Option<u64> = None;
    let mut stream = source.subscribe(spec_rx.clone(), ResumeFrom::Latest);
    while let Some(item) = stream.next().await {
        match item.expect("scripted stream must not error") {
            IngestEvent::Transaction(tx) => {
                seen.insert(tx.signature);
            }
            IngestEvent::SlotCheckpoint(cp) => cursor = Some(cp.slot),
            _ => {}
        }
    }
    assert_eq!(
        cursor,
        Some(11),
        "slot-12 work was seen but not checkpointed"
    );
    assert!(seen.contains("sig-c"));

    // Restart: resume from the persisted cursor. Inclusive semantics mean the
    // boundary slot 11 is redelivered; dedup absorbs it; slot 12 arrives.
    let mut stream = source.subscribe(spec_rx, ResumeFrom::Slot(cursor.unwrap()));
    let mut redelivered = Vec::new();
    while let Some(item) = stream.next().await {
        if let IngestEvent::Transaction(tx) = item.expect("scripted stream must not error") {
            redelivered.push(tx.signature.clone());
            seen.insert(tx.signature);
        }
    }
    assert_eq!(
        redelivered,
        vec!["sig-b", "sig-c"],
        "inclusive resume, no slot-10 replay"
    );
    assert_eq!(seen.len(), 3, "dedup absorbed the boundary overlap");
}

#[tokio::test]
async fn live_spec_update_surfaces_resubscribed_status() {
    let source = scripted_source();
    let (spec_tx, spec_rx) = watch::channel(SubscriptionSpec::default());
    let mut stream = source.subscribe(spec_rx, ResumeFrom::Latest);

    // Consume a couple of events, then the registry adds a collection.
    stream.next().await.unwrap().unwrap();
    stream.next().await.unwrap().unwrap();
    let mut spec = SubscriptionSpec::default();
    spec.transactions.insert(
        "core-txs".to_string(),
        TransactionFilter {
            account_include: vec!["CoreCollection1111111111111111111111111111".to_string()],
            ..Default::default()
        },
    );
    spec_tx.send(spec).unwrap();

    let mut saw_resubscribed = false;
    while let Some(item) = stream.next().await {
        if let IngestEvent::Status(StreamStatus::Resubscribed) =
            item.expect("scripted stream must not error")
        {
            saw_resubscribed = true;
        }
    }
    assert!(
        saw_resubscribed,
        "spec change must surface as Status(Resubscribed)"
    );
}
