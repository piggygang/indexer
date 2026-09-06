//! Global smart search (ALG-626).
//!
//! The text half only. Routing (`a pasted mint beats a wallet with holdings,
//! which beats an exact collection slug`) is a composition of three existing
//! lookups — [`crate::nft::by_address`], [`crate::wallet::total_count`] and
//! [`crate::registry::by_slug`] — and belongs to the handler that applies the
//! precedence, not here.
//!
//! One statement returns every group: window functions give each collection its
//! exact match count and a capped preview in the same pass, so a four-collection
//! search is one round trip rather than eight. The name predicate is the same
//! `ILIKE '%q%'` the browse grid already runs (11.9 ms p95 over 10,000 assets),
//! which is why there is no trigram index.

use sqlx::{FromRow, PgPool};

use crate::browse::AssetCard;

/// How the raw input was parsed. `Address` never reaches this module — an
/// address resolves by lookup and yields no groups at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Match {
    /// `#N` or a bare integer — the token number.
    Number(i32),
    /// Case-insensitive substring of the name.
    Text(String),
}

/// One matching card, tagged with its collection and that collection's exact
/// match count.
///
/// No `Eq`: the embedded card carries an `Option<f64>` rarity score.
#[derive(Debug, Clone, PartialEq, FromRow)]
pub struct SearchHit {
    pub collection_id: i32,
    /// Exact number of matches in this collection — every match, not just the
    /// ones in the capped preview.
    pub total: i64,
    #[sqlx(flatten)]
    pub card: AssetCard,
}

/// Text or number hits across every enabled collection, at most `per_group`
/// cards each, ordered by collection then token number.
///
/// The browse population applies here too (`membership_status = 'member'`,
/// burned included) so a search hit and the grid card it links to can never
/// disagree about what exists.
pub async fn grouped(
    pool: &PgPool,
    matcher: &Match,
    collection: Option<i32>,
    per_group: i64,
) -> sqlx::Result<Vec<SearchHit>> {
    let predicate = match matcher {
        Match::Number(_) => "a.number = $1::int",
        Match::Text(_) => "a.name ILIKE $1::text",
    };
    let sql = format!(
        "WITH matched AS ( \
             SELECT a.collection_id, a.id, a.address, a.name, a.number, a.image_uri, \
                    a.image_status, a.burned, a.owner, a.last_activity_at, \
                    a.rarity_score, a.rarity_rank, \
                    count(*) OVER (PARTITION BY a.collection_id)::bigint AS total, \
                    row_number() OVER (PARTITION BY a.collection_id \
                                       ORDER BY coalesce(a.number, 2147483647), a.id) AS rn \
               FROM assets a JOIN collections c ON c.id = a.collection_id \
              WHERE c.enabled AND a.membership_status = 'member' \
                AND {predicate} \
                AND ($2::int IS NULL OR a.collection_id = $2)) \
         SELECT collection_id, total, id, address, name, number, image_uri, image_status, \
                burned, owner, last_activity_at, rarity_score, rarity_rank, \
                0::bigint AS sort_number, ''::text AS sort_text \
           FROM matched WHERE rn <= $3 ORDER BY collection_id, rn"
    );
    let mut q = sqlx::query_as::<_, SearchHit>(&sql);
    q = match matcher {
        Match::Number(n) => q.bind(*n),
        Match::Text(text) => q.bind(format!("%{}%", escape_like(text))),
    };
    q.bind(collection)
        .bind(per_group)
        .persistent(false)
        .fetch_all(pool)
        .await
}

/// Escapes the `LIKE` metacharacters so a search for `100%` is a literal one.
fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn like_metacharacters_are_literal() {
        assert_eq!(escape_like("100%"), "100\\%");
        assert_eq!(escape_like("a_b"), "a\\_b");
        assert_eq!(escape_like("c:\\x"), "c:\\\\x");
    }
}
