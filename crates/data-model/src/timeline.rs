//! Activity and ownership feeds (ALG-626).
//!
//! Three keyset pages, all newest-first, each `ORDER BY` matching the index
//! the activity migration created for it:
//!
//! | feed | index |
//! |---|---|
//! | `/nfts/{id}/activity` | `activity_asset_timeline (asset_id, slot DESC, id DESC)` |
//! | `/collections/{slug}/activity` | `activity_collection_slot (collection_id, slot DESC, id DESC)` |
//! | `/nfts/{id}/owners` | `ownership_history_asset (asset_id, from_slot DESC, id DESC)` |
//!
//! Every page asks for one row more than the caller wants, so `hasMore` is
//! never a `COUNT` — the contract forbids one on these envelopes.
//!
//! `openedBySignature`/`closedBySignature` come from the `opened_by`/`closed_by`
//! foreign keys, not from a signature lookup: there is no index on
//! `activity.signature`, and `ON DELETE SET NULL` means a reclassification
//! legitimately leaves them null, which the contract renders as "acquired in an
//! unknown transaction".

use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};

use crate::browse::AssetCard;

/// One row of an activity feed — the contract's `ActivityEvent`, before the
/// HTTP layer turns `price_lamports` into a decimal string.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct ActivityEvent {
    pub id: i64,
    pub kind: String,
    pub signature: String,
    pub seq: i16,
    pub slot: i64,
    pub block_time: DateTime<Utc>,
    pub from_owner: Option<String>,
    pub to_owner: Option<String>,
    pub price_lamports: Option<i64>,
    pub marketplace: Option<String>,
}

/// A collection-feed row: the event plus the card it happened to, so a
/// recent-activity strip renders without a second request.
///
/// Flat rather than two nested structs because both halves carry an `id` and
/// sqlx's `flatten` has no column prefix; [`CollectionEvent::card`] puts the
/// NFT half back into the shape the summary DTO already consumes.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct CollectionEvent {
    pub id: i64,
    pub kind: String,
    pub signature: String,
    pub seq: i16,
    pub slot: i64,
    pub block_time: DateTime<Utc>,
    pub from_owner: Option<String>,
    pub to_owner: Option<String>,
    pub price_lamports: Option<i64>,
    pub marketplace: Option<String>,
    pub nft_id: i64,
    pub nft_address: String,
    pub nft_name: String,
    pub nft_number: Option<i32>,
    pub nft_image_uri: Option<String>,
    pub nft_image_status: String,
    pub nft_burned: bool,
    pub nft_owner: Option<String>,
    pub nft_last_activity_at: Option<DateTime<Utc>>,
}

impl CollectionEvent {
    pub fn event(&self) -> ActivityEvent {
        ActivityEvent {
            id: self.id,
            kind: self.kind.clone(),
            signature: self.signature.clone(),
            seq: self.seq,
            slot: self.slot,
            block_time: self.block_time,
            from_owner: self.from_owner.clone(),
            to_owner: self.to_owner.clone(),
            price_lamports: self.price_lamports,
            marketplace: self.marketplace.clone(),
        }
    }

    /// The embedded `NftSummary`. The sort columns are a browse-cursor concern
    /// and carry no meaning in a feed, so they are zeroed rather than selected.
    pub fn card(&self) -> AssetCard {
        AssetCard {
            id: self.nft_id,
            address: self.nft_address.clone(),
            name: self.nft_name.clone(),
            number: self.nft_number,
            image_uri: self.nft_image_uri.clone(),
            image_status: self.nft_image_status.clone(),
            burned: self.nft_burned,
            owner: self.nft_owner.clone(),
            last_activity_at: self.nft_last_activity_at,
            sort_number: 0,
            sort_text: String::new(),
        }
    }
}

/// One ownership interval — the contract's `OwnershipInterval`.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct OwnershipInterval {
    pub id: i64,
    pub owner: String,
    pub from_slot: i64,
    pub from_ts: DateTime<Utc>,
    pub to_slot: Option<i64>,
    pub to_ts: Option<DateTime<Utc>>,
    pub opened_by_signature: Option<String>,
    pub closed_by_signature: Option<String>,
}

impl OwnershipInterval {
    /// True exactly when the interval is open — the contract's `isCurrent`.
    pub fn is_current(&self) -> bool {
        self.to_slot.is_none()
    }
}

/// A keyset position in a newest-first feed: the ordering column of the last
/// row returned, plus its id as the tiebreaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Position {
    pub key: i64,
    pub id: i64,
}

const EVENT_COLUMNS: &str = "e.id, e.kind, e.signature, e.seq, e.slot, e.block_time, \
     e.from_owner, e.to_owner, e.price_lamports, e.marketplace";

/// One page of an asset's timeline, newest first.
pub async fn asset_activity(
    pool: &PgPool,
    asset_id: i64,
    kinds: &[String],
    after: Option<Position>,
    limit: i64,
) -> sqlx::Result<Vec<ActivityEvent>> {
    let (keyset, limit_param) = keyset_fragment("e.slot", "e.id", after.is_some(), 3);
    let sql = format!(
        "SELECT {EVENT_COLUMNS} FROM activity e \
          WHERE e.asset_id = $1 AND e.kind = ANY($2::text[]){keyset} \
          ORDER BY e.slot DESC, e.id DESC LIMIT {limit_param}"
    );
    let mut q = sqlx::query_as::<_, ActivityEvent>(&sql)
        .bind(asset_id)
        .bind(kinds);
    if let Some(after) = after {
        q = q.bind(after.key).bind(after.id);
    }
    q.bind(limit).fetch_all(pool).await
}

/// One page of a collection's feed, newest first, each event carrying its card.
pub async fn collection_activity(
    pool: &PgPool,
    collection_id: i32,
    kinds: &[String],
    after: Option<Position>,
    limit: i64,
) -> sqlx::Result<Vec<CollectionEvent>> {
    let (keyset, limit_param) = keyset_fragment("e.slot", "e.id", after.is_some(), 3);
    let sql = format!(
        "SELECT {EVENT_COLUMNS}, \
                a.id AS nft_id, a.address AS nft_address, a.name AS nft_name, \
                a.number AS nft_number, a.image_uri AS nft_image_uri, \
                a.image_status AS nft_image_status, a.burned AS nft_burned, \
                a.owner AS nft_owner, a.last_activity_at AS nft_last_activity_at \
           FROM activity e JOIN assets a ON a.id = e.asset_id \
          WHERE e.collection_id = $1 AND e.kind = ANY($2::text[]){keyset} \
          ORDER BY e.slot DESC, e.id DESC LIMIT {limit_param}"
    );
    let mut q = sqlx::query_as::<_, CollectionEvent>(&sql)
        .bind(collection_id)
        .bind(kinds);
    if let Some(after) = after {
        q = q.bind(after.key).bind(after.id);
    }
    q.bind(limit).fetch_all(pool).await
}

/// One page of an asset's ownership history, newest first.
pub async fn owners(
    pool: &PgPool,
    asset_id: i64,
    after: Option<Position>,
    limit: i64,
) -> sqlx::Result<Vec<OwnershipInterval>> {
    let (keyset, limit_param) = keyset_fragment("h.from_slot", "h.id", after.is_some(), 2);
    let sql = format!(
        "SELECT h.id, h.owner, h.from_slot, h.from_ts, h.to_slot, h.to_ts, \
                o.signature AS opened_by_signature, c.signature AS closed_by_signature \
           FROM ownership_history h \
           LEFT JOIN activity o ON o.id = h.opened_by \
           LEFT JOIN activity c ON c.id = h.closed_by \
          WHERE h.asset_id = $1{keyset} \
          ORDER BY h.from_slot DESC, h.id DESC LIMIT {limit_param}"
    );
    let mut q = sqlx::query_as::<_, OwnershipInterval>(&sql).bind(asset_id);
    if let Some(after) = after {
        q = q.bind(after.key).bind(after.id);
    }
    q.bind(limit).fetch_all(pool).await
}

/// The `(key, id) < (…)` clause and the limit's placeholder.
///
/// Postgres numbers placeholders by appearance, so the limit's index depends on
/// whether the keyset consumed two — the same accounting `browse` does. `next`
/// is the first free placeholder before the keyset.
fn keyset_fragment(key: &str, id: &str, has_cursor: bool, next: usize) -> (String, String) {
    if has_cursor {
        (
            format!(
                " AND ({key}, {id}) < (${next}::bigint, ${}::bigint)",
                next + 1
            ),
            format!("${}", next + 2),
        )
    } else {
        (String::new(), format!("${next}"))
    }
}
