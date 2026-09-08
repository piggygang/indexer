//! Operational commands: migrations, the registry seed, the DAS backfill, the
//! historical activity backfill, the ownership rebuild and the facet benchmark. Runs from a workstation (`DATABASE_URL` = local
//! compose or Railway's public URL) or as a one-off Railway job
//! (`BIN=indexer-admin`).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use clap::{Parser, Subcommand};
use indexer_activity::{self as activity_backfill, Venues};
use indexer_config::Config;
use indexer_das::backfill::{self, BackfillOptions};
use indexer_das::DasClient;
use indexer_data_model::activity;
use indexer_data_model::assets as data_model_assets;
use indexer_data_model::browse;
use indexer_data_model::facets::{self, TraitSelection};
use indexer_data_model::rarity;
use indexer_data_model::seed::{self, Outcome};
use indexer_data_model::synth::{self, SyntheticSpec};
use indexer_data_model::{registry, PgPool};

#[derive(Parser)]
#[command(
    name = "indexer-admin",
    version,
    about = "Operational commands: migrations, registry seed, DAS backfill, ownership rebuild, benchmarks"
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
    /// Sync the Helius webhook's address list to the registry's tracked set.
    ///
    /// Manual, like `seed`, and for a sharper reason: every Helius webhook
    /// mutation costs 100 credits and rewrites the whole list, so this lists
    /// first, diffs, and writes nothing when nothing changed. It is also the
    /// only way to register at all — the dashboard caps at 25 addresses.
    Webhook {
        /// Override `WEBHOOK_URL`.
        #[arg(long)]
        url: Option<String>,
        /// Report the diff and call nothing. Costs no credits.
        #[arg(long)]
        dry_run: bool,
        /// Fail when anything would change — the idempotency check.
        #[arg(long)]
        expect_unchanged: bool,
        /// Delete the registered webhook instead of syncing it.
        #[arg(long)]
        delete: bool,
    },
    /// DAS backfill: assets, attributes and owners (ALG-621). Idempotent —
    /// re-running an unchanged collection writes nothing and fetches nothing.
    Backfill {
        /// Only this collection (default: every enabled collection).
        #[arg(long)]
        slug: Option<String>,
        /// Continue from backfill_state instead of restarting at the first member.
        #[arg(long)]
        resume: bool,
        /// Stop after this many members per collection (smoke run; leaves status = running).
        #[arg(long)]
        limit: Option<usize>,
        /// Members per DAS call and per transaction (DAS caps getAssetBatch at 1000).
        #[arg(long, default_value_t = 1000)]
        batch: usize,
        /// Concurrent off-chain metadata fetches.
        #[arg(long, default_value_t = 16)]
        fetch_concurrency: usize,
        /// Skip the off-chain metadata fetch — write only what DAS returns.
        #[arg(long)]
        das_only: bool,
        /// Re-fetch documents even when metadata_source_uri already matches.
        #[arg(long)]
        refetch_documents: bool,
        /// Extra pass: probe image_uri reachability, set image_status/image_checked_at.
        #[arg(long)]
        check_images: bool,
        /// Re-probe images checked longer ago than this (with --check-images).
        #[arg(long, default_value_t = 30)]
        recheck_images_after_days: i32,
        /// Fail when anything would change — the "re-running changes nothing" proof.
        #[arg(long)]
        expect_unchanged: bool,
    },
    /// Historical activity backfill (ALG-622): the full transaction timeline
    /// per NFT from archival RPC — mints, transfers, priced sales, burns, and
    /// the ownership intervals derived from them.
    ///
    /// Adaptive: one archival call covers most assets, and the crawl expands
    /// to an asset's token accounts only when the timeline it derived
    /// disagrees with itself or with DAS. Idempotent — re-running a crawled
    /// collection writes nothing.
    BackfillActivity {
        /// Only this collection (default: every enabled collection).
        #[arg(long)]
        slug: Option<String>,
        /// Crawl one asset by address and stop — the hand-verification path.
        #[arg(long)]
        address: Option<String>,
        /// Continue from backfill_state instead of restarting at the first asset.
        #[arg(long)]
        resume: bool,
        /// Stop after this many assets per collection (smoke run).
        #[arg(long)]
        limit: Option<usize>,
        /// Assets per cursor commit.
        #[arg(long, default_value_t = 25)]
        batch: usize,
        /// Concurrent per-asset crawls (the RPC rate limit is shared across them).
        #[arg(long, default_value_t = 4)]
        concurrency: usize,
        /// Archival RPC calls per second. The Helius Developer plan allows 10.
        #[arg(long, default_value_t = 10)]
        rps: u32,
        /// Venue registry: marketplace program id -> label.
        #[arg(long, default_value = "config/marketplaces.toml")]
        marketplaces: PathBuf,
        /// Throw away each asset's derived rows and re-derive them. The raw
        /// signatures survive, so this re-fetches nothing it already has.
        #[arg(long)]
        reclassify: bool,
        /// Database-only: promote stored transfers to sales using the current
        /// venue registry. No network at all — use it after adding a venue.
        #[arg(long)]
        reprice_only: bool,
        /// Repair exactly the assets whose ownership is right but whose
        /// timeline is not — the transfers the live stream dropped. Targeted
        /// at the disagreement rather than at a whole collection; pair it with
        /// `--limit` to bound one run.
        #[arg(long)]
        drifted: bool,
        /// Fail when anything would change — the "re-running changes nothing" proof.
        #[arg(long)]
        expect_unchanged: bool,
    },
    /// Re-derive ownership intervals from stored activity for assets the live
    /// pipeline flagged (`ownership_dirty`), then clear the flag.
    ///
    /// This is the repair half of the writer contract: an out-of-order event is
    /// stored but not applied, and something has to rebuild the history.
    /// ALG-622 still owns classifying historical signatures — this only
    /// re-derives from what is already classified.
    RebuildOwnership {
        /// One asset by address, instead of the flagged ones.
        #[arg(long)]
        address: Option<String>,
        /// Stop after this many assets.
        #[arg(long, default_value_t = 500)]
        limit: i64,
        /// Report what would be rebuilt without writing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Recompute statistical rarity scores and 1..N ranks (ALG-627).
    ///
    /// The formula lives in `migrations/20260906000800_rarity.sql` and is
    /// implemented once, in `data-model::rarity`. A pass is idempotent: the
    /// writer's `IS DISTINCT FROM` guard means re-running an unchanged
    /// collection writes no row, which is what `--expect-unchanged` proves.
    Rarity {
        /// Only this collection (default: every enabled one).
        #[arg(long)]
        slug: Option<String>,
        /// Only collections a writer flagged — what the ingester's drain does.
        #[arg(long)]
        dirty_only: bool,
        /// Recompute and report without writing.
        #[arg(long)]
        dry_run: bool,
        /// Fail when anything would change — the "re-running changes nothing" proof.
        #[arg(long)]
        expect_unchanged: bool,
        /// Recompute a second way, in exact integer arithmetic, and compare
        /// with what is stored. This is the acceptance criterion, runnable
        /// against production.
        #[arg(long)]
        verify: bool,
        /// Print one asset's score term by term instead of recomputing.
        #[arg(long)]
        explain: Option<String>,
    },
    /// Synthetic data + facet timings — the ALG-619 "< 100 ms" acceptance evidence.
    Bench {
        /// Assets per synthetic collection (bench-pgg gets half, plus a unique trait).
        #[arg(long, default_value_t = 10_000)]
        assets: i64,
        /// Samples per scenario. p95 is nearest-rank, so below ~100 it is
        /// just the maximum and says nothing about the tail.
        #[arg(long, default_value_t = 200)]
        iterations: u32,
        /// Fail (non-zero exit) when any scenario's p95 exceeds this.
        /// ALG-625's acceptance criterion is 150 ms on the full dataset.
        #[arg(long, default_value_t = 150)]
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
        Cmd::Backfill {
            slug,
            resume,
            limit,
            batch,
            fetch_concurrency,
            das_only,
            refetch_documents,
            check_images,
            recheck_images_after_days,
            expect_unchanged,
        } => {
            // Resolved here, not at boot: `migrate` and `seed` must keep
            // working on a machine with no Helius key.
            let das = DasClient::new(config.helius.required_api_key()?)?;
            let options = BackfillOptions {
                slug,
                resume,
                limit,
                batch,
                fetch_concurrency,
                das_only,
                refetch_documents,
                check_images,
                recheck_images_after_days,
            };
            let report = backfill::run(&pool, &das, &options, print_batch_progress).await?;
            print_backfill_report(&report);

            if expect_unchanged && !report.is_noop() {
                bail!(
                    "backfill was expected to be a no-op but changed: {:?}",
                    report.totals()
                );
            }
            if let Some(failed) = report
                .collections
                .iter()
                .find(|c| c.status == "failed")
                .map(|c| c.slug.clone())
            {
                bail!("backfill failed for {failed} (see backfill_state.last_error)");
            }
        }
        Cmd::BackfillActivity {
            slug,
            address,
            resume,
            limit,
            batch,
            concurrency,
            rps,
            marketplaces,
            reclassify,
            reprice_only,
            expect_unchanged,
            drifted,
        } => {
            let venues = Venues::load(&marketplaces)?;
            println!(
                "venues  {} marketplace(s) from {}",
                venues.len(),
                marketplaces.display()
            );

            // `--reprice-only` is a database pass: the price the classifier
            // derived at crawl time is in `details`, so no key is needed.
            let das = if reprice_only {
                DasClient::with_endpoint("http://127.0.0.1:1", "")?
            } else {
                DasClient::new(config.helius.required_api_key()?)?.with_rate_limit(rps)
            };
            let options = activity_backfill::Options {
                slug,
                address,
                resume,
                limit,
                batch,
                concurrency,
                reclassify,
                reprice_only,
                drifted,
            };
            let report =
                activity_backfill::run(&pool, &das, &venues, &options, print_activity_progress)
                    .await?;
            print_activity_report(&report);

            if expect_unchanged && !report.is_noop() {
                bail!(
                    "activity backfill was expected to be a no-op but changed: {:?}",
                    report.totals()
                );
            }
            if let Some(failed) = report
                .collections
                .iter()
                .find(|c| c.status == "failed")
                .map(|c| c.slug.clone())
            {
                bail!("activity backfill failed for {failed} (see backfill_state.last_error)");
            }
        }
        Cmd::Webhook {
            url,
            dry_run,
            expect_unchanged,
            delete,
        } => {
            webhook(&pool, &config, url, dry_run, expect_unchanged, delete).await?;
        }
        Cmd::Rarity {
            slug,
            dirty_only,
            dry_run,
            expect_unchanged,
            verify,
            explain,
        } => {
            if let Some(address) = &explain {
                let terms = rarity::explain(&pool, address).await?;
                if terms.is_empty() {
                    bail!("no ranked asset with address {address}");
                }
                println!(
                    "{:<16} {:<22} {:>9} {:>14}",
                    "trait", "value", "carriers", "term"
                );
                for term in &terms {
                    println!(
                        "{:<16} {:<22} {:>9} {:>14.6}",
                        term.trait_type,
                        term.value.as_deref().unwrap_or("(absent)"),
                        term.carriers,
                        term.term
                    );
                }
                println!(
                    "{:<16} {:<22} {:>9} {:>14.6}",
                    "",
                    "SCORE",
                    "",
                    terms.iter().map(|t| t.term).sum::<f64>()
                );
                return Ok(());
            }

            let targets = rarity_targets(&pool, slug.as_deref(), dirty_only).await?;
            if targets.is_empty() {
                println!("no collection to rank");
            }
            let mut changed_slugs: Vec<String> = Vec::new();
            let mut clean = true;
            for (id, slug) in &targets {
                if !rarity::is_rankable(&pool, *id).await? {
                    // Two different situations, and only one is worth a warning.
                    // A collection with no assets is simply not backfilled yet.
                    // A collection with assets but no facetable trait type is
                    // Pig Mud: its metadata host is gone, so the only trait it
                    // carries is a per-asset-unique `Name` the registry excludes
                    // from facets. Null is the honest answer there — ranking it
                    // would mean one N-way tie — but an operator should know.
                    let members = data_model_assets::member_count(&pool, *id).await?;
                    if members > 0 {
                        log::warn!(
                            "{slug}: {members} asset(s) but no facetable trait type — rarity \
                             stays null; add a metadata_uri_template in \
                             config/collections.toml once the metadata is re-hosted"
                        );
                    } else {
                        println!("{slug:<18} not backfilled yet, nothing to rank");
                    }
                    // The pass looked and there is nothing to do. Clearing the
                    // flag matters for `--dirty-only`, which the ingester's
                    // drain mirrors: left set, it would re-examine the same
                    // collection every tick forever.
                    if !dry_run {
                        rarity::clear_dirty(&pool, *id).await?;
                    }
                    continue;
                }
                let outcome = if dry_run {
                    rarity::preview(&pool, *id).await?
                } else {
                    rarity::recompute(&pool, *id).await?
                };
                println!(
                    "{:<18} ranked={:<6} changed={:<6} version={:<4}{}",
                    slug,
                    outcome.ranked,
                    outcome.changed,
                    outcome.version,
                    if outcome.skipped {
                        " (skipped: another pass holds the lock)"
                    } else {
                        ""
                    }
                );
                if outcome.changed > 0 {
                    changed_slugs.push(slug.clone());
                }
                if verify {
                    let check = rarity::verify(&pool, *id).await?;
                    println!(
                        "{:<18} verify: {} member(s), {} ranked, {} score / {} rank mismatch(es){}",
                        slug,
                        check.members,
                        check.ranked,
                        check.score_mismatches,
                        check.rank_mismatches,
                        check
                            .first
                            .as_deref()
                            .map(|f| format!(" — {f}"))
                            .unwrap_or_default()
                    );
                    clean &= check.is_clean();
                }
            }
            if dry_run {
                println!("\n(dry run, rolled back)");
            }
            if expect_unchanged && !changed_slugs.is_empty() {
                bail!(
                    "rarity changed {} collection(s): {}",
                    changed_slugs.len(),
                    changed_slugs.join(", ")
                );
            }
            if expect_unchanged {
                println!("\nrarity is a no-op, as expected");
            }
            if verify && !clean {
                bail!("the stored ranks disagree with an independent recomputation");
            }
            if verify {
                println!("\nranks match an independent recomputation");
            }
        }
        Cmd::RebuildOwnership {
            address,
            limit,
            dry_run,
        } => {
            let targets = match &address {
                Some(address) => activity::assets_by_address(&pool, std::slice::from_ref(address))
                    .await?
                    .into_iter()
                    .collect::<Vec<_>>(),
                None => activity::dirty_assets(&pool, limit).await?,
            };
            if targets.is_empty() {
                println!("nothing to rebuild");
            }
            let mut totals = (0u64, 0u64);
            for asset in &targets {
                let mut tx = pool.begin().await?;
                let rebuilt = activity::rebuild_ownership(&mut tx, asset.id).await?;
                if dry_run {
                    tx.rollback().await?;
                } else {
                    tx.commit().await?;
                }
                totals.0 += rebuilt.events;
                totals.1 += rebuilt.intervals;
                println!(
                    "{:<44} events={:<5} intervals={:<5} was_dirty={}",
                    asset.address, rebuilt.events, rebuilt.intervals, rebuilt.was_dirty
                );
            }
            println!(
                "\nrebuilt {} asset(s): {} events -> {} intervals{}",
                targets.len(),
                totals.0,
                totals.1,
                if dry_run {
                    " (dry run, rolled back)"
                } else {
                    ""
                }
            );
            let remaining = activity::dirty_count(&pool).await?;
            println!("{remaining} asset(s) still flagged");
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

/// One line per committed batch, so a long run is legible while it happens
/// and `railway logs` shows the same thing a local terminal does.
/// One line per committed batch of the activity crawl.
fn print_activity_progress(p: &activity_backfill::BatchProgress) {
    let c = p.counts;
    println!(
        "{:<17} {:>4} assets  sigs {:>6} events {:>5} sales {:>4} \
expanded {:>4} rebuilt {:>4} mismatch {:>3}  {:>6.1}s",
        p.slug,
        p.assets,
        c.signatures,
        c.events,
        c.sales,
        c.expanded,
        c.rebuilt,
        c.mismatched,
        p.elapsed.as_secs_f64(),
    );
}

fn print_activity_report(report: &activity_backfill::Report) {
    println!();
    for collection in &report.collections {
        let c = collection.counts;
        println!(
            "{:<17} {:<16} {:>6} assets  {:>7} sigs  {:>6} events  {:>5} sales  \
{:>5} repriced  {:>6.1}s",
            collection.slug,
            collection.status,
            c.assets,
            c.signatures,
            c.events,
            c.sales,
            c.repriced,
            collection.elapsed.as_secs_f64(),
        );
        for warning in &collection.warnings {
            println!("  WARN {warning}");
        }
    }
    let totals = report.totals();
    println!(
        "\ntotal            {:>6} assets  {:>7} sigs  {:>6} events  {:>5} sales  \
{:>5} repriced",
        totals.assets, totals.signatures, totals.events, totals.sales, totals.repriced
    );
    // The acceptance criterion, printed where the operator will see it.
    println!(
        "cross-check      {} expanded to token accounts, {} still disagree with DAS, \
{} unverifiable, {} rebuilt",
        totals.expanded, totals.mismatched, totals.unverifiable, totals.rebuilt
    );
    for warning in &report.warnings {
        println!("WARN {warning}");
    }
}

fn print_batch_progress(p: &backfill::BatchProgress) {
    let of = match p.batches {
        Some(total) => format!("{:>3}/{:<3}", p.batch, total),
        // A dynamic Core collection has no known size until it is walked.
        None => format!("{:>3}/?  ", p.batch),
    };
    println!(
        "{:<17} batch {of}  slot {:<12} ins {:>5} upd {:>5} unch {:>5} miss {:<4} \
docs {}/{}  attrs +{} -{}  {:>6.1}s",
        p.slug,
        p.slot,
        p.counts.inserted,
        p.counts.updated,
        p.counts.unchanged,
        p.missing,
        p.documents_wanted - p.documents_failed,
        p.documents_wanted,
        p.counts.attributes_written,
        p.counts.attributes_removed,
        p.elapsed.as_secs_f64(),
    );
}

fn print_backfill_report(report: &backfill::BackfillReport) {
    println!(
        "\n{:<17} {:<16} {:>8} {:>9} {:>8} {:>8} {:>8} {:>6} {:>7} {:>9}",
        "collection",
        "rule",
        "members",
        "inserted",
        "updated",
        "unchang",
        "missing",
        "docs",
        "attrs",
        "elapsed"
    );
    for c in &report.collections {
        println!(
            "{:<17} {:<16} {:>8} {:>9} {:>8} {:>8} {:>8} {:>6} {:>7} {:>8.1}s  {}",
            c.slug,
            format!("{:?}", c.rule),
            c.members,
            c.counts.inserted,
            c.counts.updated,
            c.counts.unchanged,
            c.missing_total,
            c.counts.documents,
            c.counts.attributes_written,
            c.elapsed.as_secs_f64(),
            c.status,
        );
    }
    for c in &report.collections {
        if c.images_ok + c.images_dead > 0 {
            println!(
                "images     {:<17} ok={} dead={}",
                c.slug, c.images_ok, c.images_dead
            );
        }
        // The exact count is always reported; a sample of the ids makes it
        // actionable without inventing rows to make supply reconcile.
        if c.missing_total > 0 {
            let sample: Vec<&str> = c.missing.iter().take(5).map(String::as_str).collect();
            println!(
                "WARN {}: {} member(s) unknown to DAS, e.g. {}",
                c.slug,
                c.missing_total,
                sample.join(", ")
            );
        }
        for w in &c.warnings {
            println!("WARN {w}");
        }
    }
    for w in &report.warnings {
        println!("WARN {w}");
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
    /// How the page is ordered. Every sort is a scan of its own index, so the
    /// rarity index gets measured like the rest rather than assumed cheap.
    sort: browse::Sort,
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
            sort: browse::Sort::Number,
        },
        Scenario {
            name: "sort=rarity, no filters".into(),
            filters: BTreeMap::new(),
            q: None,
            sort: browse::Sort::Rarity,
        },
        Scenario {
            name: format!("{a} in {{2 most common}} AND {b} = p30 value"),
            filters: BTreeMap::from([
                (a.clone(), vec![av[0].clone(), av[1].clone()]),
                (b.clone(), vec![at(bv, 30)]),
            ]),
            q: None,
            sort: browse::Sort::Number,
        },
        Scenario {
            name: format!("{a} = p30, {b} = p30, {c} = rarest"),
            filters: BTreeMap::from([
                (a.clone(), vec![at(av, 30)]),
                (b.clone(), vec![at(bv, 30)]),
                (c.clone(), vec![cv[cv.len() - 1].clone()]),
            ]),
            q: None,
            sort: browse::Sort::Number,
        },
        Scenario {
            name: "text search q=#12".into(),
            filters: BTreeMap::new(),
            q: Some("#12"),
            sort: browse::Sort::Number,
        },
    ])
}

/// The collections a `rarity` run covers: one slug, the flagged ones, or every
/// enabled collection in registry order.
async fn rarity_targets(
    pool: &PgPool,
    slug: Option<&str>,
    dirty_only: bool,
) -> anyhow::Result<Vec<(i32, String)>> {
    if let Some(slug) = slug {
        let row = registry::by_slug(pool, slug)
            .await?
            .with_context(|| format!("no collection with slug {slug}"))?;
        return Ok(vec![(row.id, row.slug)]);
    }
    let enabled = registry::list_enabled(pool).await?;
    if !dirty_only {
        return Ok(enabled.into_iter().map(|c| (c.id, c.slug)).collect());
    }
    let dirty = rarity::dirty_collections(pool).await?;
    Ok(enabled
        .into_iter()
        .filter(|c| dirty.contains(&c.id))
        .map(|c| (c.id, c.slug))
        .collect())
}

async fn seed_bench_collections(pool: &PgPool, assets: i64) -> anyhow::Result<()> {
    let specs = [
        SyntheticSpec {
            slug: "bench-psg".into(),
            name: "Bench PSG-like".into(),
            assets,
            unique_trait: false,
            coverage: 1.0,
            seed: 0.42,
        },
        SyntheticSpec {
            slug: "bench-pgg".into(),
            name: "Bench PGG-like".into(),
            assets: assets / 2,
            unique_trait: true,
            coverage: 1.0,
            seed: 0.43,
        },
        SyntheticSpec {
            slug: "bench-core".into(),
            name: "Bench Core-like".into(),
            assets,
            unique_trait: false,
            // Sparse, like the dynamic Core collection it stands in for: the
            // browse benchmark's rarity scenario is only representative if
            // some assets are missing some traits.
            coverage: 0.35,
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
            // The browse page under the same filters — the grid and the
            // sidebar are one request pair in the Explorer, so timing only the
            // facets would understate what a page load costs.
            let browse_query = browse::BrowseQuery {
                collection_id: collection.id,
                selections: selections.clone(),
                q: scenario.q.map(str::to_owned),
                sort: scenario.sort,
                after: None,
                limit: 25,
            };
            let mut page_samples = Vec::with_capacity(options.iterations as usize);
            for _ in 0..options.iterations.max(1) {
                let started = Instant::now();
                browse::browse(pool, &browse_query).await?;
                page_samples.push(started.elapsed().as_secs_f64() * 1000.0);
            }
            page_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let page_p95 =
                page_samples[(page_samples.len() * 95 / 100).min(page_samples.len() - 1)];

            samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let p50 = samples[samples.len() / 2];
            let p95 = samples[(samples.len() * 95 / 100).min(samples.len() - 1)];
            let max = samples[samples.len() - 1];
            // Gated on p95, not p50: a median under budget with a fat tail is
            // exactly the shape a browse endpoint must not ship.
            let over = p95 > options.max_ms as f64;
            let verdict = if over { "FAIL" } else { "ok" };
            println!(
                "{verdict:<4} {:<52} p50 {p50:7.1} ms  p95 {p95:7.1} ms  max {max:7.1} ms  \
                 rows {rows}  page p95 {page_p95:6.1} ms",
                scenario.name
            );
            if page_p95 > options.max_ms as f64 {
                failed.push(format!(
                    "{slug}: {} browse p95 {page_p95:.1} ms",
                    scenario.name
                ));
            }
            if over {
                failed.push(format!("{slug}: {} p95 {p95:.1} ms", scenario.name));
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
            "p95 exceeded {} ms:\n  {}",
            options.max_ms,
            failed.join("\n  ")
        );
    }
    println!("\nall scenarios under {} ms (p95)", options.max_ms);
    Ok(())
}

/// Syncs the Helius webhook's `accountAddresses` to the registry's tracked set.
///
/// Three properties, all of them about not spending credits:
///
/// * **List first, always.** `GET /v0/webhooks` is free, and it is what tells
///   us whether a webhook exists, what it holds, and — because a wrong
///   `HELIUS_WEBHOOK_API` host fails right here — whether we are even talking
///   to Helius. Nothing is written before it succeeds.
/// * **A no-op writes nothing.** Every mutation costs 100 credits and rewrites
///   the whole list, so an unchanged registry must cost zero. That is what
///   makes this safe to run from a deploy script or a habit.
/// * **The address set comes from `registry::tracked_addresses`**, the same
///   function the WebSocket's subscription spec uses. A second derivation is
///   how the registered list silently falls behind the registry — and a drifted
///   webhook looks perfectly healthy while delivering nothing.
async fn webhook(
    pool: &PgPool,
    config: &Config,
    url: Option<String>,
    dry_run: bool,
    expect_unchanged: bool,
    delete: bool,
) -> anyhow::Result<()> {
    use indexer_das::webhooks::{Diff, WebhookClient};

    let api_key = config.helius.required_api_key()?;
    let url = match url {
        Some(url) => url,
        None => config.helius.required_webhook_url()?.to_string(),
    };
    let client = WebhookClient::new(&config.helius.webhook_api, api_key)?;

    let registered = client.list().await.with_context(|| {
        format!(
            "listing webhooks at {} — check HELIUS_WEBHOOK_API and HELIUS_API_KEY",
            config.helius.webhook_api
        )
    })?;
    // Matched by URL rather than by id: the id is Helius's, the URL is ours,
    // and it is what a second environment would differ by.
    let existing = registered.iter().find(|w| w.webhook_url == url);
    println!(
        "{} webhook(s) registered; {} for {url}",
        registered.len(),
        if existing.is_some() {
            "one matches"
        } else {
            "none match"
        }
    );

    if delete {
        let Some(existing) = existing else {
            println!("nothing to delete");
            return Ok(());
        };
        if dry_run {
            println!(
                "dry run: would delete {} (100 credits)",
                existing.webhook_id
            );
            return Ok(());
        }
        client.delete(&existing.webhook_id).await?;
        println!("deleted {}", existing.webhook_id);
        return Ok(());
    }

    let wanted = registry::tracked_addresses(pool).await?;
    if wanted.is_empty() {
        bail!("the registry tracks no addresses — run `seed` (and a backfill) first");
    }
    if wanted.len() > indexer_das::webhooks::MAX_ADDRESSES {
        bail!(
            "{} addresses exceeds Helius's limit of {}",
            wanted.len(),
            indexer_das::webhooks::MAX_ADDRESSES
        );
    }

    let diff = match existing {
        Some(existing) => Diff::between(existing, &url, &wanted),
        // Nothing registered: everything is an addition.
        None => Diff {
            added: wanted.clone(),
            removed: Vec::new(),
            url_changed: true,
            kept: 0,
        },
    };
    println!(
        "tracked={} registered={} +{} -{} kept={}{}",
        wanted.len(),
        existing.map_or(0, |w| w.account_addresses.len()),
        diff.added.len(),
        diff.removed.len(),
        diff.kept,
        if diff.url_changed { " url-changed" } else { "" }
    );
    for address in diff.added.iter().take(5) {
        println!("  + {address}");
    }
    for address in diff.removed.iter().take(5) {
        println!("  - {address}");
    }

    if diff.is_noop() {
        println!("unchanged — no request made, no credits spent");
        return Ok(());
    }
    if expect_unchanged {
        bail!(
            "--expect-unchanged: {} addition(s), {} removal(s){}",
            diff.added.len(),
            diff.removed.len(),
            if diff.url_changed {
                ", url changed"
            } else {
                ""
            }
        );
    }
    if dry_run {
        println!(
            "dry run: would write {} address(es) (100 credits)",
            wanted.len()
        );
        return Ok(());
    }

    // The secret is written, never read back: Helius does not return it on
    // every route, so the only way to be sure the registered value matches the
    // one the receiver checks is to set it on every write.
    let secret = config.helius.required_webhook_secret()?;
    let result = match existing {
        Some(existing) => {
            client
                .update(&existing.webhook_id, &url, &wanted, secret)
                .await?
        }
        None => client.create(&url, &wanted, secret).await?,
    };
    println!(
        "{} {} with {} address(es) (100 credits)",
        if existing.is_some() {
            "updated"
        } else {
            "created"
        },
        result.webhook_id,
        wanted.len()
    );
    Ok(())
}
