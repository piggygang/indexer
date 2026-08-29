//! Synthetic dataset generator for benchmarks and tests. Produces disabled
//! `bench-*` collections with realistic trait cardinalities (Zipf-ish value
//! skew), ~2% burned assets, ~3k skewed owners, one mint event per asset and
//! transfers on ~30% — enough for the facet, stats and `-activity` paths.
//! Optionally a per-asset-unique `Name` trait (the Piggy Girl Gang pathology),
//! excluded from facets through `collections.facet_exclude` like real data.
//!
//! Addresses and signatures are md5-derived base58-alphabet strings prefixed
//! `SYN` / `HLD` — valid for the CHECKs, obviously synthetic. Everything is
//! deterministic for a given `seed`.

use anyhow::Context;
use sqlx::PgPool;

use crate::attributes::ensure_trait_type;

/// Trait types with Piggy SOL Gang's real cardinalities (dressme manifest).
pub const FACET_TRAITS: [(&str, i32); 7] = [
    ("Background", 7),
    ("Body", 7),
    ("Clothes", 13),
    ("Eyes", 16),
    ("Mouth", 12),
    ("Head", 23),
    ("Earring", 10),
];

/// The per-asset-unique trait type (value = the asset name, `#N`).
pub const UNIQUE_TRAIT: &str = "Name";

#[derive(Debug, Clone)]
pub struct SyntheticSpec {
    /// Must start with `bench-` so `clean` can find it.
    pub slug: String,
    pub name: String,
    pub assets: i64,
    pub unique_trait: bool,
    /// `setseed` argument in `[-1, 1]`.
    pub seed: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyntheticReport {
    pub collection_id: i32,
    pub assets: i64,
    pub attributes: i64,
    /// `false` when the collection already had assets and was left as is.
    pub generated: bool,
}

/// Value ordinal skew: `floor(power(random(), SKEW) * n)` — ordinal 0 is the
/// most common value, the tail is rare.
const SKEW: f64 = 2.2;

pub async fn seed_synthetic(
    pool: &PgPool,
    spec: &SyntheticSpec,
) -> anyhow::Result<SyntheticReport> {
    anyhow::ensure!(
        spec.slug.starts_with("bench-"),
        "synthetic slugs must start with bench-"
    );
    let mut tx = pool.begin().await?;

    sqlx::query("SELECT setseed($1)")
        .bind(spec.seed)
        .execute(&mut *tx)
        .await?;

    let facet_exclude: Vec<&str> = if spec.unique_trait {
        vec![UNIQUE_TRAIT]
    } else {
        vec![]
    };
    let collection_id: i32 = sqlx::query_scalar(
        "INSERT INTO collections (slug, name, standard, enabled, facet_exclude) \
         VALUES ($1, $2, 'core', false, $3) \
         ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name, facet_exclude = EXCLUDED.facet_exclude \
         RETURNING id",
    )
    .bind(&spec.slug)
    .bind(&spec.name)
    .bind(&facet_exclude)
    .fetch_one(&mut *tx)
    .await?;

    let existing: i64 = sqlx::query_scalar("SELECT count(*) FROM assets WHERE collection_id = $1")
        .bind(collection_id)
        .fetch_one(&mut *tx)
        .await?;
    if existing > 0 {
        let attributes: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM asset_attributes aa JOIN assets a ON a.id = aa.asset_id WHERE a.collection_id = $1",
        )
        .bind(collection_id)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        return Ok(SyntheticReport {
            collection_id,
            assets: existing,
            attributes,
            generated: false,
        });
    }

    sqlx::query(
        "INSERT INTO assets (address, collection_id, name, symbol, metadata_uri, owner, owner_slot, burned) \
         SELECT 'SYN' || translate(md5($1::text || ':' || s.g::text), '0', 'Z'), \
                $1, '#' || s.g, 'SYN', 'synthetic://' || $1 || '/' || s.g, \
                CASE WHEN s.burned THEN NULL ELSE 'HLD' || translate(md5('owner:' || s.k::text), '0', 'Z') END, \
                CASE WHEN s.burned THEN NULL ELSE 300000000 + s.g * 10 END, \
                s.burned \
           FROM (SELECT g, random() < 0.02 AS burned, floor(power(random(), 2) * 3000)::int AS k \
                   FROM generate_series(1, $2::int) g) s",
    )
    .bind(collection_id)
    .bind(spec.assets)
    .execute(&mut *tx)
    .await
    .context("generating assets")?;

    for (position, (name, cardinality)) in FACET_TRAITS.iter().enumerate() {
        let trait_type_id = ensure_trait_type(&mut *tx, collection_id, name).await?;
        sqlx::query(
            "INSERT INTO trait_values (trait_type_id, value) \
             SELECT $1, 'v' || lpad(g::text, 3, '0') FROM generate_series(1, $2::int) g \
             ON CONFLICT DO NOTHING",
        )
        .bind(trait_type_id)
        .bind(cardinality)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "WITH vals AS (SELECT id, trait_type_id, row_number() OVER (ORDER BY id) - 1 AS ord \
                             FROM trait_values WHERE trait_type_id = $2), \
                  pick AS (SELECT a.id AS asset_id, floor(power(random(), $4::float8) * $3::int)::int AS ord \
                             FROM assets a WHERE a.collection_id = $1) \
             INSERT INTO asset_attributes (asset_id, collection_id, trait_type_id, trait_value_id, position) \
             SELECT p.asset_id, $1, v.trait_type_id, v.id, $5::smallint \
               FROM pick p JOIN vals v ON v.ord = p.ord",
        )
        .bind(collection_id)
        .bind(trait_type_id)
        .bind(cardinality)
        .bind(SKEW)
        .bind(position as i16)
        .execute(&mut *tx)
        .await
        .with_context(|| format!("generating {name} attributes"))?;
    }

    if spec.unique_trait {
        let trait_type_id = ensure_trait_type(&mut *tx, collection_id, UNIQUE_TRAIT).await?;
        sqlx::query(
            "INSERT INTO trait_values (trait_type_id, value) \
             SELECT $1, '#' || g FROM generate_series(1, $2::int) g ON CONFLICT DO NOTHING",
        )
        .bind(trait_type_id)
        .bind(spec.assets)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO asset_attributes (asset_id, collection_id, trait_type_id, trait_value_id, position) \
             SELECT a.id, $1, $2, tv.id, $3::smallint \
               FROM assets a JOIN trait_values tv ON tv.trait_type_id = $2 AND tv.value = a.name \
              WHERE a.collection_id = $1",
        )
        .bind(collection_id)
        .bind(trait_type_id)
        .bind(FACET_TRAITS.len() as i16)
        .execute(&mut *tx)
        .await
        .context("generating unique-trait attributes")?;
    }

    // One mint per asset, then a transfer on ~30%. Signatures are 88 chars
    // of md5-derived base58 alphabet; slots are distinct per asset so the
    // last_activity trigger has an order to follow.
    sqlx::query(
        "INSERT INTO activity (asset_id, collection_id, signature, seq, slot, block_time, kind, from_owner, to_owner, source) \
         SELECT a.id, $1, \
                substr(translate(md5('mint:' || a.id) || md5('mint2:' || a.id) || md5('mint3:' || a.id), '0', 'Z'), 1, 88), \
                0, 300000000 + a.id * 10, now() - interval '400 days' + (a.id % 100000) * interval '1 minute', \
                'mint', NULL, coalesce(a.owner, 'HLD' || translate(md5('owner:0'), '0', 'Z')), 'manual' \
           FROM assets a WHERE a.collection_id = $1",
    )
    .bind(collection_id)
    .execute(&mut *tx)
    .await
    .context("generating mint events")?;
    sqlx::query(
        "INSERT INTO activity (asset_id, collection_id, signature, seq, slot, block_time, kind, from_owner, to_owner, source) \
         SELECT a.id, $1, \
                substr(translate(md5('xfer:' || a.id) || md5('xfer2:' || a.id) || md5('xfer3:' || a.id), '0', 'Z'), 1, 88), \
                0, 300000000 + a.id * 10 + 5, now() - interval '200 days' + (a.id % 100000) * interval '1 minute', \
                'transfer', 'HLD' || translate(md5('owner:0'), '0', 'Z'), \
                coalesce(a.owner, 'HLD' || translate(md5('owner:1'), '0', 'Z')), 'manual' \
           FROM assets a WHERE a.collection_id = $1 AND random() < 0.3",
    )
    .bind(collection_id)
    .execute(&mut *tx)
    .await
    .context("generating transfer events")?;

    sqlx::query(
        "ANALYZE collections, assets, trait_types, trait_values, asset_attributes, activity",
    )
    .execute(&mut *tx)
    .await?;

    let attributes: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM asset_attributes aa JOIN assets a ON a.id = aa.asset_id WHERE a.collection_id = $1",
    )
    .bind(collection_id)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(SyntheticReport {
        collection_id,
        assets: spec.assets,
        attributes,
        generated: true,
    })
}

/// Deletes every `bench-*` collection with its assets (cascades attributes,
/// activity, ownership, signatures). Returns the number of collections removed.
pub async fn clean(pool: &PgPool) -> anyhow::Result<u64> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        "DELETE FROM assets WHERE collection_id IN (SELECT id FROM collections WHERE slug LIKE 'bench-%')",
    )
    .execute(&mut *tx)
    .await?;
    let removed = sqlx::query("DELETE FROM collections WHERE slug LIKE 'bench-%'")
        .execute(&mut *tx)
        .await?
        .rows_affected();
    tx.commit().await?;
    Ok(removed)
}
