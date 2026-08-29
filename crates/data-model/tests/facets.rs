//! The disjunctive facet query against a brute-force recomputation over
//! synthetic data, plus the `facet_counts` view semantics.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use indexer_data_model::facets::{self, FacetCount, TraitSelection};
use indexer_data_model::synth::{self, SyntheticSpec};
use indexer_data_model::PgPool;

#[derive(Debug, Default)]
struct Asset {
    burned: bool,
    name: String,
    number: Option<i32>,
    /// (trait_type_id, trait_value_id, is_facet)
    attrs: Vec<(i32, i32, bool)>,
}

#[derive(sqlx::FromRow)]
struct AttrRow {
    id: i64,
    burned: bool,
    name: String,
    number: Option<i32>,
    trait_type_id: i32,
    trait_value_id: i32,
    is_facet: bool,
}

async fn dump(pool: &PgPool, collection_id: i32) -> HashMap<i64, Asset> {
    let rows: Vec<AttrRow> = sqlx::query_as(
        "SELECT a.id, a.burned, a.name, a.number, aa.trait_type_id, aa.trait_value_id, tt.is_facet \
           FROM assets a \
           JOIN asset_attributes aa ON aa.asset_id = a.id \
           JOIN trait_types tt ON tt.id = aa.trait_type_id \
          WHERE a.collection_id = $1",
    )
    .bind(collection_id)
    .fetch_all(pool)
    .await
    .unwrap();
    let mut assets: HashMap<i64, Asset> = HashMap::new();
    for r in rows {
        let a = assets.entry(r.id).or_default();
        a.burned = r.burned;
        a.name = r.name;
        a.number = r.number;
        a.attrs
            .push((r.trait_type_id, r.trait_value_id, r.is_facet));
    }
    assets
}

fn matches_q(a: &Asset, q: Option<&str>) -> bool {
    let Some(q) = q else { return true };
    let numeric = q.trim_start_matches('#').parse::<i32>().ok();
    a.name.to_lowercase().contains(&q.to_lowercase()) || (numeric.is_some() && a.number == numeric)
}

/// Disjunctive counts by definition over the browse population (member
/// assets, burned included): for trait type T, apply every selected type's
/// filter except T's own.
fn brute_force(
    assets: &HashMap<i64, Asset>,
    selections: &[TraitSelection],
    q: Option<&str>,
) -> BTreeMap<(i32, i32), i64> {
    let selected: BTreeMap<i32, BTreeSet<i32>> = selections
        .iter()
        .map(|s| (s.trait_type_id, s.trait_value_ids.iter().copied().collect()))
        .collect();
    let mut counts = BTreeMap::new();
    for a in assets.values() {
        if !matches_q(a, q) {
            continue;
        }
        let missing: BTreeSet<i32> = selected
            .iter()
            .filter(|(t, values)| {
                !a.attrs
                    .iter()
                    .any(|(at, av, _)| at == *t && values.contains(av))
            })
            .map(|(t, _)| *t)
            .collect();
        for (t, v, facet) in &a.attrs {
            if !facet {
                continue;
            }
            let counted = missing.is_empty() || (missing.len() == 1 && missing.contains(t));
            if counted {
                *counts.entry((*t, *v)).or_insert(0) += 1;
            }
        }
    }
    counts
}

fn as_map(rows: &[FacetCount]) -> BTreeMap<(i32, i32), i64> {
    rows.iter()
        .map(|r| ((r.trait_type_id, r.trait_value_id), r.count))
        .collect()
}

fn filters(pairs: &[(&str, &[&str])]) -> BTreeMap<String, Vec<String>> {
    pairs
        .iter()
        .map(|(t, vs)| (t.to_string(), vs.iter().map(|v| v.to_string()).collect()))
        .collect()
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn disjunctive_counts_match_brute_force(pool: PgPool) {
    let spec = SyntheticSpec {
        slug: "bench-test-pgg".into(),
        name: "Test PGG-like".into(),
        assets: 1_500,
        unique_trait: true,
        seed: 0.17,
    };
    let report = synth::seed_synthetic(&pool, &spec).await.unwrap();
    assert!(report.generated);
    assert_eq!(
        report.attributes,
        1_500 * 8,
        "7 facet traits + the unique Name"
    );
    let assets = dump(&pool, report.collection_id).await;
    assert_eq!(assets.len(), 1_500);

    type Scenario = (
        &'static str,
        BTreeMap<String, Vec<String>>,
        Option<&'static str>,
    );
    let scenarios: Vec<Scenario> = vec![
        ("no filters", BTreeMap::new(), None),
        (
            "one type, two values",
            filters(&[("Background", &["v001", "v002"])]),
            None,
        ),
        (
            "two types",
            filters(&[("Background", &["v001", "v002"]), ("Head", &["v003"])]),
            None,
        ),
        (
            "three types, one rare",
            filters(&[
                ("Background", &["v001"]),
                ("Eyes", &["v002"]),
                ("Earring", &["v009"]),
            ]),
            None,
        ),
        (
            "unknown value matches nothing",
            filters(&[("Background", &["v001"]), ("Eyes", &["nope"])]),
            None,
        ),
        ("text search", BTreeMap::new(), Some("#12")),
        (
            "text search + filter",
            filters(&[("Body", &["v001"])]),
            Some("#1"),
        ),
    ];
    for (name, f, q) in scenarios {
        let selections = facets::resolve_selections(&pool, report.collection_id, &f)
            .await
            .unwrap()
            .expect("known trait types");
        let actual = facets::disjunctive_facet_counts(&pool, report.collection_id, &selections, q)
            .await
            .unwrap();
        let expected = brute_force(&assets, &selections, q);
        assert_eq!(as_map(&actual), expected, "scenario {name}");
        assert!(
            !actual.iter().any(|r| r.trait_type == "Name"),
            "unique trait never faceted ({name})"
        );
        // Ordered by trait type name, then count desc, then value.
        let sorted = actual.windows(2).all(|w| {
            (&w[0].trait_type, -w[0].count, &w[0].value)
                <= (&w[1].trait_type, -w[1].count, &w[1].value)
        });
        assert!(sorted, "scenario {name} ordering");
    }

    // Unknown trait TYPE: nothing can match.
    assert!(
        facets::resolve_selections(&pool, report.collection_id, &filters(&[("Hat", &["x"])]))
            .await
            .unwrap()
            .is_none()
    );

    // The unfiltered view equals the disjunctive query with no selection and
    // excludes burned assets: every facet type sums to the live population.
    let view = facets::facet_counts(&pool, report.collection_id)
        .await
        .unwrap();
    let plain = facets::disjunctive_facet_counts(&pool, report.collection_id, &[], None)
        .await
        .unwrap();
    assert_eq!(as_map(&view), as_map(&plain));
    // Burned assets stay in the population (the grid shows them greyed), so
    // every facet type sums to the full membership; only `supply` drops.
    let burned = assets.values().filter(|a| a.burned).count() as i64;
    assert!(burned > 0, "the generator burns a few assets");
    let mut per_type: BTreeMap<i32, i64> = BTreeMap::new();
    for r in &view {
        *per_type.entry(r.trait_type_id).or_insert(0) += r.count;
    }
    assert_eq!(per_type.len(), 7);
    assert!(per_type.values().all(|sum| *sum == 1_500), "{per_type:?}");
    let supply: i32 =
        sqlx::query_scalar("SELECT supply FROM collection_stats WHERE collection_id = $1")
            .bind(report.collection_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(supply as i64, 1_500 - burned);
}

#[sqlx::test]
#[ignore = "needs DATABASE_URL"]
async fn synth_is_deterministic_and_cleanable(pool: PgPool) {
    let spec = SyntheticSpec {
        slug: "bench-test-psg".into(),
        name: "Test PSG-like".into(),
        assets: 300,
        unique_trait: false,
        seed: 0.5,
    };
    let first = synth::seed_synthetic(&pool, &spec).await.unwrap();
    assert!(first.generated);
    let snapshot = facets::facet_counts(&pool, first.collection_id)
        .await
        .unwrap();
    // Re-running leaves existing data alone.
    let again = synth::seed_synthetic(&pool, &spec).await.unwrap();
    assert!(!again.generated);
    assert_eq!(
        facets::facet_counts(&pool, first.collection_id)
            .await
            .unwrap(),
        snapshot
    );
    // Same seed after a clean regenerates identical counts.
    assert_eq!(synth::clean(&pool).await.unwrap(), 1);
    let rebuilt = synth::seed_synthetic(&pool, &spec).await.unwrap();
    assert!(rebuilt.generated);
    let rebuilt_counts = facets::facet_counts(&pool, rebuilt.collection_id)
        .await
        .unwrap();
    let strip = |rows: &[FacetCount]| -> Vec<(String, String, i64)> {
        rows.iter()
            .map(|r| (r.trait_type.clone(), r.value.clone(), r.count))
            .collect()
    };
    assert_eq!(strip(&rebuilt_counts), strip(&snapshot));
    let stats: (i32, i32, i32) = sqlx::query_as(
        "SELECT supply, holders, activity_7d FROM collection_stats WHERE collection_id = $1",
    )
    .bind(rebuilt.collection_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(stats.0 > 280 && stats.0 <= 300, "supply {}", stats.0);
    assert!(stats.1 > 0 && stats.1 <= stats.0, "holders {}", stats.1);
    assert_eq!(stats.2, 0, "synthetic activity is dated months back");
}
