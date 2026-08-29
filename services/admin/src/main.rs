//! Operational commands: migrations, the registry seed and the facet
//! benchmark. Runs from a workstation (`DATABASE_URL` = local compose or
//! Railway's public URL) or as a one-off Railway job (`BIN=indexer-admin`).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use clap::{Parser, Subcommand};
use indexer_config::Config;
use indexer_data_model::facets::{self, TraitSelection};
use indexer_data_model::seed::{self, Outcome};
use indexer_data_model::synth::{self, SyntheticSpec};
use indexer_data_model::{registry, PgPool};

#[derive(Parser)]
#[command(
    name = "indexer-admin",
    version,
    about = "Operational commands: migrations, registry seed, benchmarks"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Apply pending migrations (the same embedded migrator indexer-api runs at boot).
    Migrate,
    /// Apply config/collections.toml (+ mint lists) idempotently. Never deletes.
    Seed {
        #[arg(long, default_value = "config/collections.toml")]
        config: PathBuf,
        /// Validate and report, then roll back.
        #[arg(long)]
        dry_run: bool,
        /// Fail when anything would change — CI's idempotency check.
        #[arg(long)]
        expect_unchanged: bool,
        /// Permit changing standard/address/verified_creator of a collection that has assets.
        #[arg(long)]
        allow_identity_change: bool,
    },
    /// Synthetic data + facet timings — the ALG-619 "< 100 ms" acceptance evidence.
    Bench {
        /// Assets per synthetic collection (bench-pgg gets half, plus a unique trait).
        #[arg(long, default_value_t = 10_000)]
        assets: i64,
        #[arg(long, default_value_t = 20)]
        iterations: u32,
        /// Fail (non-zero exit) when any scenario's p50 exceeds this.
        #[arg(long, default_value_t = 100)]
        max_ms: u64,
        /// Remove every bench-* collection instead of benchmarking.
        #[arg(long)]
        clean: bool,
        /// Benchmark an existing (real) collection instead of the synthetic ones.
        #[arg(long)]
        slug: Option<String>,
        /// Touch 20% of the assets first (no VACUUM) so index-only scans degrade like in production.
        #[arg(long)]
        dirty: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    let cli = Cli::parse();

    let config = Config::try_from_env()?;
    let db = &config.database;
    let pool = indexer_data_model::connect(
        db.required_url()?,
        db.max_connections,
        Duration::from_secs(db.connect_timeout_secs),
    )
    .await?;

    match cli.cmd {
        Cmd::Migrate => {
            indexer_data_model::migrate(&pool).await?;
            println!("migrations up to date");
        }
        Cmd::Seed {
            config,
            dry_run,
            expect_unchanged,
            allow_identity_change,
        } => {
            // A seed against an unmigrated database is a footgun.
            indexer_data_model::migrate(&pool).await?;
            let seed = seed::load(&config)?;
            let options = seed::ApplyOptions {
                dry_run,
                allow_identity_change,
            };
            let report = seed::apply(&pool, &seed, options).await?;
            print_seed_report(&report);
            if expect_unchanged {
                let changed: Vec<String> = report
                    .collections
                    .iter()
                    .filter(|c| {
                        c.outcome != Outcome::Unchanged || c.mints_new > 0 || c.facets_synced > 0
                    })
                    .map(|c| c.slug.clone())
                    .chain(
                        report
                            .tokens
                            .iter()
                            .filter(|t| t.outcome != Outcome::Unchanged)
                            .map(|t| t.mint.clone()),
                    )
                    .collect();
                if !changed.is_empty() {
                    bail!(
                        "seed was expected to be a no-op but changed: {}",
                        changed.join(", ")
                    );
                }
                println!("seed is a no-op, as expected");
            }
        }
        Cmd::Bench {
            assets,
            iterations,
            max_ms,
            clean,
            slug,
            dirty,
        } => {
            let options = BenchOptions {
                assets,
                iterations,
                max_ms,
                clean,
                slug,
                dirty,
            };
            bench(&pool, options).await?
        }
    }
    Ok(())
}

fn outcome(o: Outcome) -> &'static str {
    match o {
        Outcome::Inserted => "inserted",
        Outcome::Updated => "updated",
        Outcome::Unchanged => "unchanged",
    }
}

fn print_seed_report(report: &seed::SeedReport) {
    for c in &report.collections {
        println!(
            "collection {:<18} id={:<3} {:<9} mints: file={} new={} total={} facets_synced={}",
            c.slug,
            c.id,
            outcome(c.outcome),
            c.mints_in_file,
            c.mints_new,
            c.mints_total,
            c.facets_synced
        );
    }
    for t in &report.tokens {
        println!("token      {:<44} {}", t.mint, outcome(t.outcome));
    }
    for w in &report.warnings {
        println!("WARN {w}");
    }
    if report.dry_run {
        println!("dry run: rolled back, nothing persisted");
    }
}

struct BenchOptions {
    assets: i64,
    iterations: u32,
    max_ms: u64,
    clean: bool,
    slug: Option<String>,
    dirty: bool,
}

struct Scenario {
    name: String,
    filters: BTreeMap<String, Vec<String>>,
    q: Option<&'static str>,
}

/// Scenarios derived from the collection's own facet distribution, so the
/// same matrix is meaningful on synthetic and on real data: a common pair,
/// a three-way with one rare value, and a text search.
async fn derive_scenarios(pool: &PgPool, collection_id: i32) -> anyhow::Result<Vec<Scenario>> {
    let counts = facets::facet_counts(pool, collection_id).await?;
    // Values per type, most common first (the view is already ordered so).
    let mut by_type: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for c in &counts {
        by_type
            .entry(c.trait_type.clone())
            .or_default()
            .push(c.value.clone());
    }
    let types: Vec<(&String, &Vec<String>)> =
        by_type.iter().filter(|(_, v)| v.len() >= 2).collect();
    anyhow::ensure!(
        types.len() >= 3,
        "need at least three facet trait types with 2+ values"
    );
    let at = |values: &Vec<String>, pct: usize| {
        values[(values.len() * pct / 100).min(values.len() - 1)].clone()
    };
    let (a, av) = types[0];
    let (b, bv) = types[1];
    let (c, cv) = types[2];
    Ok(vec![
        Scenario {
            name: "no filters".into(),
            filters: BTreeMap::new(),
            q: None,
        },
        Scenario {
            name: format!("{a} in {{2 most common}} AND {b} = p30 value"),
            filters: BTreeMap::from([
                (a.clone(), vec![av[0].clone(), av[1].clone()]),
                (b.clone(), vec![at(bv, 30)]),
            ]),
            q: None,
        },
        Scenario {
            name: format!("{a} = p30, {b} = p30, {c} = rarest"),
            filters: BTreeMap::from([
                (a.clone(), vec![at(av, 30)]),
                (b.clone(), vec![at(bv, 30)]),
                (c.clone(), vec![cv[cv.len() - 1].clone()]),
            ]),
            q: None,
        },
        Scenario {
            name: "text search q=#12".into(),
            filters: BTreeMap::new(),
            q: Some("#12"),
        },
    ])
}

async fn seed_bench_collections(pool: &PgPool, assets: i64) -> anyhow::Result<()> {
    let specs = [
        SyntheticSpec {
            slug: "bench-psg".into(),
            name: "Bench PSG-like".into(),
            assets,
            unique_trait: false,
            seed: 0.42,
        },
        SyntheticSpec {
            slug: "bench-pgg".into(),
            name: "Bench PGG-like".into(),
            assets: assets / 2,
            unique_trait: true,
            seed: 0.43,
        },
        SyntheticSpec {
            slug: "bench-core".into(),
            name: "Bench Core-like".into(),
            assets,
            unique_trait: false,
            seed: 0.44,
        },
    ];
    for spec in &specs {
        let started = Instant::now();
        let r = synth::seed_synthetic(pool, spec).await?;
        println!(
            "{:<11} id={:<3} assets={:<6} attributes={:<7} {}",
            spec.slug,
            r.collection_id,
            r.assets,
            r.attributes,
            if r.generated {
                format!("generated in {:.1}s", started.elapsed().as_secs_f64())
            } else {
                "already present".into()
            }
        );
    }
    Ok(())
}

async fn bench(pool: &PgPool, options: BenchOptions) -> anyhow::Result<()> {
    if options.clean {
        let removed = synth::clean(pool).await?;
        println!("removed {removed} bench collection(s)");
        return Ok(());
    }
    let slugs: Vec<String> = match options.slug {
        Some(slug) => vec![slug],
        None => {
            seed_bench_collections(pool, options.assets).await?;
            vec!["bench-psg".into(), "bench-pgg".into(), "bench-core".into()]
        }
    };

    let mut failed = Vec::new();
    for slug in &slugs {
        let collection = registry::by_slug(pool, slug)
            .await?
            .with_context(|| format!("collection {slug} not found"))?;
        if options.dirty {
            let touched = indexer_data_model::touch_assets_for_bench(pool, collection.id).await?;
            println!("dirtied {touched} asset rows (no VACUUM)");
        }
        println!("\n== {slug} (collection {}) ==", collection.id);
        let mut explain_for: Option<Vec<TraitSelection>> = None;
        for scenario in derive_scenarios(pool, collection.id).await? {
            let Some(selections) =
                facets::resolve_selections(pool, collection.id, &scenario.filters).await?
            else {
                bail!(
                    "{slug}: scenario '{}' references an unknown trait type",
                    scenario.name
                );
            };
            for _ in 0..2 {
                facets::disjunctive_facet_counts(pool, collection.id, &selections, scenario.q)
                    .await?;
            }
            let mut samples = Vec::with_capacity(options.iterations as usize);
            let mut rows = 0;
            for _ in 0..options.iterations.max(1) {
                let started = Instant::now();
                rows =
                    facets::disjunctive_facet_counts(pool, collection.id, &selections, scenario.q)
                        .await?
                        .len();
                samples.push(started.elapsed().as_secs_f64() * 1000.0);
            }
            samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let p50 = samples[samples.len() / 2];
            let p95 = samples[(samples.len() * 95 / 100).min(samples.len() - 1)];
            let max = samples[samples.len() - 1];
            let over = p50 > options.max_ms as f64;
            let verdict = if over { "FAIL" } else { "ok" };
            println!(
                "{verdict:<4} {:<52} p50 {p50:7.1} ms  p95 {p95:7.1} ms  max {max:7.1} ms  rows {rows}",
                scenario.name
            );
            if over {
                failed.push(format!("{slug}: {} p50 {p50:.1} ms", scenario.name));
            }
            if scenario.filters.len() == 2 {
                explain_for = Some(selections);
            }
        }
        if let Some(selections) = explain_for {
            println!("\nEXPLAIN (ANALYZE, BUFFERS) — two active types:");
            for line in facets::explain_disjunctive(pool, collection.id, &selections, None).await? {
                println!("  {line}");
            }
        }
    }
    if !failed.is_empty() {
        bail!(
            "facet p50 exceeded {} ms:\n  {}",
            options.max_ms,
            failed.join("\n  ")
        );
    }
    println!("\nall scenarios under {} ms (p50)", options.max_ms);
    Ok(())
}
