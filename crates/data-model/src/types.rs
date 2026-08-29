//! Text-backed enums. Postgres stores them as `text + CHECK`; the sqlx derive
//! declares the type by name (`text`), which sqlx matches against TEXT
//! columns by name, so they bind and decode without a custom Postgres type.

use serde::{Deserialize, Serialize};

/// On-chain standard — the API contract's `standard` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum Standard {
    TokenMetadata,
    Core,
}

impl Standard {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TokenMetadata => "token_metadata",
            Self::Core => "core",
        }
    }
}

/// How an on-chain asset is recognized as a member of a collection. Derived
/// by Postgres from the registry columns (`collections.membership_rule`);
/// backfill (ALG-621), the live pipeline (ALG-623) and reconciliation
/// (ALG-624) `match` on it — one arm per rule, and all three exist from day
/// one, which is what makes onboarding a collection a data change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum MembershipRule {
    /// Metaplex Core: the asset's collection is `address`. DAS: `searchAssets`
    /// grouping `["collection", address]`.
    CoreCollection,
    /// Token Metadata with a certified collection: `metadata.collection ==
    /// { key: address, verified: true }`.
    TmCollection,
    /// Token Metadata without one (candy-machine era): `creators[0] ==
    /// { verified_creator, verified: true }`, matching symbol, AND the mint
    /// is in `collection_mints`. DAS: `getAssetBatch` over the allowlist.
    TmAllowlist,
}

/// Activity event kinds. The API serves [`EventKind::PUBLIC`]; the rest are
/// stored so the classifier (ALG-622) needs no migration for them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Mint,
    Transfer,
    Sale,
    Burn,
    Stake,
    Unstake,
    Other,
}

impl EventKind {
    pub const PUBLIC: [EventKind; 4] = [
        EventKind::Mint,
        EventKind::Transfer,
        EventKind::Sale,
        EventKind::Burn,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mint => "mint",
            Self::Transfer => "transfer",
            Self::Sale => "sale",
            Self::Burn => "burn",
            Self::Stake => "stake",
            Self::Unstake => "unstake",
            Self::Other => "other",
        }
    }
}
