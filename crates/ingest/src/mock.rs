use tokio::sync::watch;

use crate::event::{
    AccountUpdate, IngestEvent, RawPayload, Slot, SlotCheckpoint, StreamStatus, TransactionUpdate,
};
use crate::source::{EventStream, IngestError, IngestSource, ResumeFrom};
use crate::spec::SubscriptionSpec;

/// Scripted [`IngestSource`] for tests. Replays its events honoring
/// [`ResumeFrom`] (inclusive), checks the spec watch channel between events
/// and injects [`StreamStatus::Resubscribed`] on a change, and optionally
/// ends with a scripted terminal error.
#[derive(Debug, Clone, Default)]
pub struct MockSource {
    events: Vec<IngestEvent>,
    terminal_error: Option<IngestError>,
}

impl MockSource {
    pub fn new(events: Vec<IngestEvent>) -> Self {
        Self {
            events,
            terminal_error: None,
        }
    }

    /// Script a terminal failure after the events are exhausted.
    pub fn end_with_error(mut self, error: IngestError) -> Self {
        self.terminal_error = Some(error);
        self
    }

    /// Minimal transaction event for scripts.
    pub fn tx(signature: &str, slot: Slot, filter: &str) -> IngestEvent {
        IngestEvent::Transaction(TransactionUpdate {
            filters: vec![filter.to_string()],
            slot,
            signature: signature.to_string(),
            failed: false,
            account_keys: Vec::new(),
            raw: RawPayload::Json(serde_json::Value::Null),
        })
    }

    /// Minimal account event for scripts.
    pub fn account(pubkey: &str, slot: Slot, filter: &str) -> IngestEvent {
        IngestEvent::Account(AccountUpdate {
            filters: vec![filter.to_string()],
            slot,
            pubkey: pubkey.to_string(),
            owner: String::new(),
            lamports: 0,
            executable: false,
            data: Vec::new(),
            write_version: None,
            txn_signature: None,
        })
    }

    pub fn checkpoint(slot: Slot) -> IngestEvent {
        IngestEvent::SlotCheckpoint(SlotCheckpoint { slot })
    }
}

impl IngestSource for MockSource {
    fn name(&self) -> &'static str {
        "mock"
    }

    fn subscribe(
        &self,
        mut spec: watch::Receiver<SubscriptionSpec>,
        resume: ResumeFrom,
    ) -> EventStream {
        let events = self.events.clone();
        let terminal_error = self.terminal_error.clone();
        Box::pin(async_stream::stream! {
            for event in events {
                if spec.has_changed().unwrap_or(false) {
                    spec.borrow_and_update();
                    yield Ok(IngestEvent::Status(StreamStatus::Resubscribed));
                }
                let deliver = match (resume, event.slot()) {
                    (ResumeFrom::Slot(from), Some(slot)) => slot >= from,
                    _ => true,
                };
                if deliver {
                    yield Ok(event);
                }
            }
            if let Some(error) = terminal_error {
                yield Err(error);
            }
        })
    }
}
