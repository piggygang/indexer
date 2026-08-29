//! `config/collections.toml` → registry. Everything is validated before the
//! first write; the apply step is one transaction: upsert collections by
//! slug (config is the source of truth for every scalar column, including
//! `enabled`), insert allowlist mints (never delete), upsert tokens, re-sync
//! `trait_types.is_facet`. Re-running with the same file is a no-op — the
//! upserts skip unchanged rows, so `updated_at` does not move either.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use serde::Deserialize;
use sqlx::{PgPool, Postgres, Transaction};

use crate::attributes::sync_trait_facets;
use crate::types::Standard;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SeedFile {
    pub version: u32,
    #[serde(default)]
    pub collections: Vec<CollectionSeed>,
    #[serde(default)]
    pub tokens: Vec<TokenSeed>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectionSeed {
    pub slug: String,
    pub name: String,
    #[serde(default)]
    pub standard: Option<Standard>,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub address: Option<String>,
    #[serde(default)]
    pub verified_creator: Option<String>,
    #[serde(default)]
    pub update_authority: Option<String>,
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default)]
    pub image_url: Option<String>,
    /// Off-chain metadata location override, `{mint}` = the asset id.
    #[serde(default)]
    pub metadata_uri_template: Option<String>,
    #[serde(default)]
    pub facet_exclude: Vec<String>,
    #[serde(default)]
    pub mints: Option<MintList>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MintList {
    /// Path to a JSON array of base58 mints, relative to the TOML file.
    pub file: PathBuf,
    /// Expected length — guards against a truncated or wrong file.
    #[serde(default)]
    pub count: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenSeed {
    pub mint: String,
    pub symbol: String,
    pub name: String,
    pub decimals: u8,
    #[serde(default)]
    pub logo_uri: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

/// A parsed and validated seed: the file plus the resolved mint lists, keyed
/// by collection slug.
#[derive(Debug, Clone)]
pub struct Seed {
    pub file: SeedFile,
    pub mints: BTreeMap<String, Vec<String>>,
}

/// Parses the TOML, reads every referenced mint list (relative to the TOML's
/// directory) and validates the whole thing. Every problem is reported at
/// once.
pub fn load(path: &Path) -> anyhow::Result<Seed> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let file: SeedFile =
        toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    let base = path.parent().unwrap_or_else(|| Path::new("."));

    let mut mints = BTreeMap::new();
    let mut problems = Vec::new();
    for collection in &file.collections {
        let Some(list) = &collection.mints else {
            continue;
        };
        let file_path = base.join(&list.file);
        match read_mint_list(&file_path) {
            Ok(entries) => {
                if let Some(expected) = list.count {
                    if entries.len() != expected {
                        problems.push(format!(
                            "{}: {} has {} mints, config says count = {expected}",
                            collection.slug,
                            file_path.display(),
                            entries.len()
                        ));
                    }
                }
                mints.insert(collection.slug.clone(), entries);
            }
            Err(e) => problems.push(format!("{}: {e:#}", collection.slug)),
        }
    }
    problems.extend(validate(&file, &mints));
    if !problems.is_empty() {
        bail!(
            "invalid seed {}:\n  - {}",
            path.display(),
            problems.join("\n  - ")
        );
    }
    Ok(Seed { file, mints })
}

fn read_mint_list(path: &Path) -> anyhow::Result<Vec<String>> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let entries: Vec<String> =
        serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    Ok(entries)
}

/// `true` when `s` is a base58 string decoding to exactly 32 bytes.
pub fn is_pubkey(s: &str) -> bool {
    matches!(bs58::decode(s).into_vec(), Ok(bytes) if bytes.len() == 32)
}

fn is_slug(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.split('-').all(|part| {
            !part.is_empty()
                && part
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        })
}

/// Mirrors the DB CHECKs plus the rules Postgres cannot express (allowlist
/// non-empty, lists disjoint). Returns every violation found.
pub fn validate(file: &SeedFile, mints: &BTreeMap<String, Vec<String>>) -> Vec<String> {
    let mut problems = Vec::new();
    if file.version != 1 {
        problems.push(format!("unsupported version {} (expected 1)", file.version));
    }

    let mut slugs = BTreeSet::new();
    let mut mint_owner: HashMap<&str, &str> = HashMap::new();
    for c in &file.collections {
        let slug = c.slug.as_str();
        let mut p = |msg: String| problems.push(format!("{slug}: {msg}"));
        if !is_slug(slug) {
            p("slug must match ^[a-z0-9]+(-[a-z0-9]+)*$ and be <= 64 chars".into());
        }
        if !slugs.insert(slug) {
            p("duplicate slug".into());
        }
        if c.name.is_empty() || c.name.len() > 128 {
            p("name must be 1..=128 chars".into());
        }
        for (field, value) in [
            ("address", &c.address),
            ("verified_creator", &c.verified_creator),
            ("update_authority", &c.update_authority),
        ] {
            if let Some(v) = value {
                if !is_pubkey(v) {
                    p(format!("{field} is not a base58 32-byte pubkey: {v}"));
                }
            }
        }
        if let Some(symbol) = &c.symbol {
            if symbol.is_empty() || symbol.len() > 10 {
                p("symbol must be 1..=10 chars".into());
            }
        }
        if c.facet_exclude.iter().any(|t| t.is_empty()) {
            p("facet_exclude entries must be non-empty".into());
        }
        if let Some(t) = &c.metadata_uri_template {
            if !t.starts_with("https://") || !t.contains("{mint}") {
                p("metadata_uri_template must be an https URL containing {mint}".into());
            }
        }
        let has_tm_fields = c.verified_creator.is_some()
            || c.symbol.is_some()
            || c.update_authority.is_some()
            || c.mints.is_some();
        match c.standard {
            Some(Standard::Core) => {
                if has_tm_fields {
                    p("a core collection cannot have verified_creator / symbol / update_authority / mints".into());
                }
                if c.enabled && c.address.is_none() {
                    p("enabled core collection needs address (CollectionV1)".into());
                }
            }
            Some(Standard::TokenMetadata) => {
                if c.enabled && c.address.is_none() && c.verified_creator.is_none() {
                    p("enabled token_metadata collection needs address (certified collection) or verified_creator".into());
                }
                if c.address.is_none() && c.verified_creator.is_some() {
                    match mints.get(slug) {
                        Some(list) if !list.is_empty() => {}
                        _ => p("verified_creator without address needs a non-empty mints list (tm_allowlist)".into()),
                    }
                }
            }
            None => {
                if c.enabled {
                    p("enabled collection needs a standard".into());
                }
                if has_tm_fields || c.address.is_some() {
                    p("a placeholder without standard cannot carry addresses or mints".into());
                }
            }
        }
        if let Some(list) = mints.get(slug) {
            let mut seen = BTreeSet::new();
            for mint in list {
                if !is_pubkey(mint) {
                    p(format!("mint is not a base58 32-byte pubkey: {mint}"));
                }
                if !seen.insert(mint.as_str()) {
                    p(format!("duplicate mint in list: {mint}"));
                }
                match mint_owner.insert(mint.as_str(), slug) {
                    Some(other) if other != slug => {
                        p(format!("mint {mint} is also listed by {other}"));
                    }
                    _ => {}
                }
            }
        }
    }

    let mut token_mints = BTreeSet::new();
    for t in &file.tokens {
        let mint = t.mint.as_str();
        if !is_pubkey(mint) {
            problems.push(format!("token {mint}: not a base58 32-byte pubkey"));
        }
        if !token_mints.insert(mint) {
            problems.push(format!("token {mint}: duplicate"));
        }
        if t.symbol.is_empty() || t.symbol.len() > 10 {
            problems.push(format!("token {mint}: symbol must be 1..=10 chars"));
        }
        if t.name.is_empty() || t.name.len() > 128 {
            problems.push(format!("token {mint}: name must be 1..=128 chars"));
        }
    }
    problems
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Inserted,
    Updated,
    Unchanged,
}

#[derive(Debug, Clone)]
pub struct CollectionOutcome {
    pub slug: String,
    pub id: i32,
    pub outcome: Outcome,
    /// Mints in the config file (0 when the collection has no list).
    pub mints_in_file: usize,
    /// Mints newly inserted by this run.
    pub mints_new: u64,
    /// Mints in the DB after this run.
    pub mints_total: i64,
    /// Trait types whose `is_facet` flag was re-synced.
    pub facets_synced: u64,
}

#[derive(Debug, Clone)]
pub struct TokenOutcome {
    pub mint: String,
    pub outcome: Outcome,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ApplyOptions {
    /// Validate and report inside a transaction that is rolled back.
    pub dry_run: bool,
    /// Permit changing `standard` / `address` / `verified_creator` on a
    /// collection that already has indexed assets. Off by default: those
    /// columns define what the existing rows ARE, and a re-seed with a typo
    /// would silently relabel thousands of NFTs.
    pub allow_identity_change: bool,
}

#[derive(Debug, Clone, Default)]
pub struct SeedReport {
    pub collections: Vec<CollectionOutcome>,
    pub tokens: Vec<TokenOutcome>,
    pub warnings: Vec<String>,
    pub dry_run: bool,
}

/// Applies the seed in one transaction. With `dry_run` the transaction is
/// rolled back at the end, so the report is exact and nothing persists.
pub async fn apply(
    pool: &PgPool,
    seed: &Seed,
    options: ApplyOptions,
) -> anyhow::Result<SeedReport> {
    let mut tx = pool.begin().await.context("starting seed transaction")?;
    let mut report = SeedReport {
        dry_run: options.dry_run,
        ..Default::default()
    };

    for c in &seed.file.collections {
        if !options.allow_identity_change {
            guard_identity(&mut tx, c).await?;
        }
        let (id, outcome) = upsert_collection(&mut tx, c)
            .await
            .with_context(|| format!("upserting collection {}", c.slug))?;
        let list = seed.mints.get(&c.slug).map(Vec::as_slice).unwrap_or(&[]);
        let mints_new = if list.is_empty() {
            0
        } else {
            insert_mints(&mut tx, id, &c.slug, list)
                .await
                .with_context(|| format!("inserting mints for {}", c.slug))?
        };
        let mints_total: i64 =
            sqlx::query_scalar("SELECT count(*) FROM collection_mints WHERE collection_id = $1")
                .bind(id)
                .fetch_one(&mut *tx)
                .await?;
        if c.mints.is_some() && mints_total > list.len() as i64 {
            report.warnings.push(format!(
                "{}: DB has {mints_total} mints, config lists {} — extras are kept (the seed never deletes)",
                c.slug,
                list.len()
            ));
        }
        let facets_synced = sync_trait_facets(&mut *tx, id).await?;
        report.collections.push(CollectionOutcome {
            slug: c.slug.clone(),
            id,
            outcome,
            mints_in_file: list.len(),
            mints_new,
            mints_total,
            facets_synced,
        });
    }

    for t in &seed.file.tokens {
        let outcome = upsert_token(&mut tx, t)
            .await
            .with_context(|| format!("upserting token {}", t.mint))?;
        report.tokens.push(TokenOutcome {
            mint: t.mint.clone(),
            outcome,
        });
    }

    let config_slugs: BTreeSet<&str> = seed
        .file
        .collections
        .iter()
        .map(|c| c.slug.as_str())
        .collect();
    let db_slugs: Vec<String> = sqlx::query_scalar("SELECT slug FROM collections ORDER BY id")
        .fetch_all(&mut *tx)
        .await?;
    for slug in db_slugs
        .iter()
        .filter(|s| !config_slugs.contains(s.as_str()))
    {
        report.warnings.push(format!(
            "collection {slug} exists in the DB but not in the config — left untouched"
        ));
    }
    let config_tokens: BTreeSet<&str> = seed.file.tokens.iter().map(|t| t.mint.as_str()).collect();
    let db_tokens: Vec<String> = sqlx::query_scalar("SELECT mint FROM tokens ORDER BY created_at")
        .fetch_all(&mut *tx)
        .await?;
    for mint in db_tokens
        .iter()
        .filter(|m| !config_tokens.contains(m.as_str()))
    {
        report.warnings.push(format!(
            "token {mint} exists in the DB but not in the config — left untouched"
        ));
    }

    if options.dry_run {
        tx.rollback().await.context("rolling back dry run")?;
    } else {
        tx.commit().await.context("committing seed")?;
    }
    Ok(report)
}

#[derive(sqlx::FromRow)]
struct IdentityRow {
    standard: Option<String>,
    address: Option<String>,
    verified_creator: Option<String>,
    has_assets: bool,
}

/// Refuses to change a collection's identity columns once assets exist.
async fn guard_identity(
    tx: &mut Transaction<'_, Postgres>,
    c: &CollectionSeed,
) -> anyhow::Result<()> {
    let existing: Option<IdentityRow> = sqlx::query_as(
        "SELECT standard, address, verified_creator, \
                EXISTS (SELECT 1 FROM assets a WHERE a.collection_id = c.id) AS has_assets \
           FROM collections c WHERE slug = $1",
    )
    .bind(&c.slug)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(IdentityRow {
        standard,
        address,
        verified_creator: creator,
        has_assets,
    }) = existing
    else {
        return Ok(());
    };
    if !has_assets {
        return Ok(());
    }
    let wanted_standard = c.standard.map(|s| s.as_str().to_owned());
    let mut changed = Vec::new();
    if standard != wanted_standard {
        changed.push(format!("standard {standard:?} -> {wanted_standard:?}"));
    }
    if address != c.address {
        changed.push(format!("address {address:?} -> {:?}", c.address));
    }
    if creator != c.verified_creator {
        changed.push(format!(
            "verified_creator {creator:?} -> {:?}",
            c.verified_creator
        ));
    }
    if changed.is_empty() {
        return Ok(());
    }
    bail!(
        "{}: refusing to change the identity of a collection that already has assets ({}); \
         re-run with --allow-identity-change if this is intended",
        c.slug,
        changed.join(", ")
    );
}

async fn upsert_collection(
    tx: &mut Transaction<'_, Postgres>,
    c: &CollectionSeed,
) -> anyhow::Result<(i32, Outcome)> {
    // The WHERE clause makes an unchanged row a true no-op (no new tuple, no
    // updated_at bump); RETURNING is then empty and the id is looked up.
    let row: Option<(i32, bool)> = sqlx::query_as(
        "INSERT INTO collections \
            (slug, name, standard, address, verified_creator, update_authority, symbol, image_url, \
             metadata_uri_template, facet_exclude, enabled) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
         ON CONFLICT (slug) DO UPDATE SET \
            name = EXCLUDED.name, standard = EXCLUDED.standard, address = EXCLUDED.address, \
            verified_creator = EXCLUDED.verified_creator, update_authority = EXCLUDED.update_authority, \
            symbol = EXCLUDED.symbol, image_url = EXCLUDED.image_url, \
            metadata_uri_template = EXCLUDED.metadata_uri_template, \
            facet_exclude = EXCLUDED.facet_exclude, enabled = EXCLUDED.enabled \
         WHERE (collections.name, collections.standard, collections.address, collections.verified_creator, \
                collections.update_authority, collections.symbol, collections.image_url, \
                collections.metadata_uri_template, collections.facet_exclude, collections.enabled) \
               IS DISTINCT FROM \
               (EXCLUDED.name, EXCLUDED.standard, EXCLUDED.address, EXCLUDED.verified_creator, \
                EXCLUDED.update_authority, EXCLUDED.symbol, EXCLUDED.image_url, \
                EXCLUDED.metadata_uri_template, EXCLUDED.facet_exclude, EXCLUDED.enabled) \
         RETURNING id, (xmax = 0) AS inserted",
    )
    .bind(&c.slug)
    .bind(&c.name)
    .bind(c.standard)
    .bind(&c.address)
    .bind(&c.verified_creator)
    .bind(&c.update_authority)
    .bind(&c.symbol)
    .bind(&c.image_url)
    .bind(&c.metadata_uri_template)
    .bind(&c.facet_exclude)
    .bind(c.enabled)
    .fetch_optional(&mut **tx)
    .await?;
    match row {
        Some((id, true)) => Ok((id, Outcome::Inserted)),
        Some((id, false)) => Ok((id, Outcome::Updated)),
        None => {
            let id: i32 = sqlx::query_scalar("SELECT id FROM collections WHERE slug = $1")
                .bind(&c.slug)
                .fetch_one(&mut **tx)
                .await?;
            Ok((id, Outcome::Unchanged))
        }
    }
}

async fn insert_mints(
    tx: &mut Transaction<'_, Postgres>,
    collection_id: i32,
    slug: &str,
    mints: &[String],
) -> anyhow::Result<u64> {
    let foreign: Vec<(String, String)> = sqlx::query_as(
        "SELECT m.mint, c.slug FROM collection_mints m JOIN collections c ON c.id = m.collection_id \
          WHERE m.mint = ANY($1) AND m.collection_id <> $2 LIMIT 5",
    )
    .bind(mints)
    .bind(collection_id)
    .fetch_all(&mut **tx)
    .await?;
    if let Some((mint, other)) = foreign.first() {
        bail!("{slug}: mint {mint} already belongs to collection {other} (a mint is in exactly one collection)");
    }
    let result = sqlx::query(
        "INSERT INTO collection_mints (mint, collection_id) \
         SELECT m, $2 FROM unnest($1::text[]) AS m \
         ON CONFLICT (mint) DO NOTHING",
    )
    .bind(mints)
    .bind(collection_id)
    .execute(&mut **tx)
    .await?;
    Ok(result.rows_affected())
}

async fn upsert_token(
    tx: &mut Transaction<'_, Postgres>,
    t: &TokenSeed,
) -> anyhow::Result<Outcome> {
    let row: Option<(bool,)> = sqlx::query_as(
        "INSERT INTO tokens (mint, symbol, name, decimals, logo_uri, enabled) \
         VALUES ($1, $2, $3, $4, $5, $6) \
         ON CONFLICT (mint) DO UPDATE SET \
            symbol = EXCLUDED.symbol, name = EXCLUDED.name, decimals = EXCLUDED.decimals, \
            logo_uri = EXCLUDED.logo_uri, enabled = EXCLUDED.enabled \
         WHERE (tokens.symbol, tokens.name, tokens.decimals, tokens.logo_uri, tokens.enabled) \
               IS DISTINCT FROM \
               (EXCLUDED.symbol, EXCLUDED.name, EXCLUDED.decimals, EXCLUDED.logo_uri, EXCLUDED.enabled) \
         RETURNING (xmax = 0) AS inserted",
    )
    .bind(&t.mint)
    .bind(&t.symbol)
    .bind(&t.name)
    .bind(i16::from(t.decimals))
    .bind(&t.logo_uri)
    .bind(t.enabled)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(match row {
        Some((true,)) => Outcome::Inserted,
        Some((false,)) => Outcome::Updated,
        None => Outcome::Unchanged,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn committed_config() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/collections.toml")
    }

    /// A broken committed config must fail plain `cargo test`, no DB needed.
    #[test]
    fn committed_config_is_valid() {
        let seed = load(&committed_config()).unwrap();
        assert_eq!(seed.file.version, 1);
        let slugs: Vec<&str> = seed
            .file
            .collections
            .iter()
            .map(|c| c.slug.as_str())
            .collect();
        assert_eq!(
            slugs,
            ["piggy-sol-gang", "piggy-girl-gang", "pig-mud", "piggy-gang"]
        );
        assert!(seed.file.collections.iter().all(|c| c.enabled));
        let total: usize = seed.mints.values().map(Vec::len).sum();
        assert_eq!(total, 10_000 + 5_000 + 2_073);
        assert_eq!(seed.file.tokens.len(), 1);
        assert_eq!(seed.file.tokens[0].decimals, 9);
        let templated: Vec<&str> = seed
            .file
            .collections
            .iter()
            .filter(|c| c.metadata_uri_template.is_some())
            .map(|c| c.slug.as_str())
            .collect();
        assert_eq!(templated, ["piggy-sol-gang", "piggy-girl-gang"]);
        assert!(
            !seed.mints.contains_key("piggy-gang"),
            "core collections carry no mint list"
        );
    }

    fn parse(toml_text: &str) -> Vec<String> {
        let file: SeedFile = toml::from_str(toml_text).unwrap();
        validate(&file, &BTreeMap::new())
    }

    /// Obviously synthetic 32-byte keys (never a real on-chain address).
    fn synthetic_pk(seed: u8) -> String {
        bs58::encode([seed; 32]).into_string()
    }

    #[test]
    fn validation_rejects_bad_registry_rows() {
        let enabled_without_rule = parse(
            r#"version = 1
[[collections]]
slug = "x"
name = "X"
standard = "token_metadata"
enabled = true"#,
        );
        assert!(enabled_without_rule
            .iter()
            .any(|p| p.contains("needs address")));

        let creator_on_core = parse(&format!(
            r#"version = 1
[[collections]]
slug = "x"
name = "X"
standard = "core"
address = "{}"
verified_creator = "{}""#,
            synthetic_pk(1),
            synthetic_pk(2)
        ));
        assert!(creator_on_core
            .iter()
            .any(|p| p.contains("core collection cannot")));

        let bad_base58 = parse(
            r#"version = 1
[[collections]]
slug = "x"
name = "X"
standard = "core"
address = "not-base58-0OIl""#,
        );
        assert!(bad_base58.iter().any(|p| p.contains("not a base58")));

        let allowlist_without_list = parse(&format!(
            r#"version = 1
[[collections]]
slug = "x"
name = "X"
standard = "token_metadata"
verified_creator = "{}""#,
            synthetic_pk(2)
        ));
        assert!(allowlist_without_list
            .iter()
            .any(|p| p.contains("non-empty mints list")));

        let placeholder = parse(
            r#"version = 1
[[collections]]
slug = "later"
name = "Later"
enabled = false"#,
        );
        assert!(placeholder.is_empty(), "{placeholder:?}");

        let bad_template = parse(&format!(
            r#"version = 1
[[collections]]
slug = "x"
name = "X"
standard = "core"
address = "{}"
metadata_uri_template = "http://example.com/meta.json""#,
            synthetic_pk(1)
        ));
        assert!(bad_template
            .iter()
            .any(|p| p.contains("metadata_uri_template")));
    }

    #[test]
    fn unknown_keys_are_rejected() {
        let err = toml::from_str::<SeedFile>(
            r#"version = 1
[[collections]]
slug = "x"
name = "X"
adress = "typo""#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("adress"), "{err}");
    }
}
