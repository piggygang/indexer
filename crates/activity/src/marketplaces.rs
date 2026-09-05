//! The venue registry: marketplace program id → label.
//!
//! Marketplace program ids are on-chain addresses, and CLAUDE.md is
//! unambiguous that those live only in `config/` — never in Rust, SQL or
//! tests. That rule is the whole reason `decode` captures every invoked
//! program id as *data*: the mapping from a program to "Magic Eden" can then
//! be a config change rather than a code change.
//!
//! This is a file of its own rather than a section of `collections.toml`, so
//! the registry seed, its identity guards and its `deny_unknown_fields` parser
//! are untouched and no migration is needed. The consequence to know: `config/`
//! ships in the **admin image only**, so this map is reachable from
//! `indexer-admin` and not from the ingester — live sale labelling would need
//! a different source.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{ensure, Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct VenueFile {
    version: u32,
    #[serde(default)]
    marketplaces: Vec<VenueSeed>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct VenueSeed {
    program_id: String,
    label: String,
    /// Free-text note — which era or program version this id is. Never read.
    #[serde(default)]
    #[allow(dead_code)]
    note: Option<String>,
}

/// Program id → venue label.
#[derive(Debug, Clone, Default)]
pub struct Venues {
    by_program: BTreeMap<String, String>,
}

impl Venues {
    /// Loads and validates the venue file.
    ///
    /// An empty registry is legal and means "price nothing": with no venue
    /// matched, every marketplace-mediated move stays an honest `transfer`,
    /// which is exactly what the live pipeline already records.
    pub fn load(path: &Path) -> Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let parsed: VenueFile =
            toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        ensure!(
            parsed.version == 1,
            "{}: unsupported version {}",
            path.display(),
            parsed.version
        );

        let mut by_program = BTreeMap::new();
        for venue in parsed.marketplaces {
            ensure!(
                !venue.label.trim().is_empty(),
                "{}: {} has an empty label",
                path.display(),
                venue.program_id
            );
            // Same shape `is_pubkey` enforces in Postgres. Checked here so a
            // typo fails the run rather than silently never matching.
            let length = venue.program_id.chars().count();
            ensure!(
                (32..=44).contains(&length)
                    && venue
                        .program_id
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() && !"0OIl".contains(c)),
                "{}: {} is not a base58 program id",
                path.display(),
                venue.program_id
            );
            if let Some(previous) = by_program.insert(venue.program_id.clone(), venue.label) {
                anyhow::bail!(
                    "{}: {} is listed twice (first as {previous})",
                    path.display(),
                    venue.program_id
                );
            }
        }
        Ok(Self { by_program })
    }

    /// The label for a program id, if it is a known venue.
    pub fn label(&self, program_id: &str) -> Option<&str> {
        self.by_program.get(program_id).map(String::as_str)
    }

    /// The first venue among a transaction's invoked programs, in execution
    /// order — the outer marketplace program runs before the token transfer
    /// it CPIs into, so first-seen is the right one.
    pub fn find<'a>(&'a self, programs: &[String]) -> Option<&'a str> {
        programs.iter().find_map(|p| self.label(p))
    }

    pub fn len(&self) -> usize {
        self.by_program.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_program.is_empty()
    }
}

/// Builds a registry without a file — tests, and any caller that already has
/// the pairs.
impl FromIterator<(String, String)> for Venues {
    fn from_iter<I: IntoIterator<Item = (String, String)>>(pairs: I) -> Self {
        Self {
            by_program: pairs.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(body: &str) -> tempfile_lite::Temp {
        tempfile_lite::Temp::with(body)
    }

    #[test]
    fn the_committed_registry_is_valid() {
        // Runs on a bare `cargo test`, like seed.rs's own config check: a typo
        // in config/marketplaces.toml must fail here, not on Railway.
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/marketplaces.toml");
        let venues = Venues::load(&path).expect("config/marketplaces.toml must parse");
        assert!(
            !venues.is_empty(),
            "an empty registry would silently price nothing"
        );
    }

    #[test]
    fn the_first_venue_in_execution_order_wins() {
        let venues: Venues = [
            ("prog-outer".to_string(), "Outer".to_string()),
            ("prog-inner".to_string(), "Inner".to_string()),
        ]
        .into_iter()
        .collect();
        // The marketplace program is invoked before the token transfer it
        // CPIs into, and `decode` records programs in execution order.
        let programs = vec!["prog-outer".to_string(), "prog-inner".to_string()];
        assert_eq!(venues.find(&programs), Some("Outer"));
        assert_eq!(venues.find(&["nothing".to_string()]), None);
    }

    #[test]
    fn a_typo_fails_loudly() {
        let bad =
            write("version = 1\n[[marketplaces]]\nprogram_id = \"not-base58!\"\nlabel = \"X\"\n");
        assert!(
            Venues::load(bad.path()).is_err(),
            "a bad program id must fail"
        );

        let dup = write(
            "version = 1\n\
             [[marketplaces]]\nprogram_id = \"11111111111111111111111111111112\"\nlabel = \"A\"\n\
             [[marketplaces]]\nprogram_id = \"11111111111111111111111111111112\"\nlabel = \"B\"\n",
        );
        assert!(Venues::load(dup.path()).is_err(), "a duplicate must fail");

        let old = write("version = 2\n");
        assert!(
            Venues::load(old.path()).is_err(),
            "an unknown version must fail"
        );
    }

    /// A two-function stand-in for the `tempfile` crate: the workspace has no
    /// temp-file dependency and this needs three throwaway files.
    mod tempfile_lite {
        use std::path::{Path, PathBuf};

        pub struct Temp(PathBuf);

        impl Temp {
            pub fn with(body: &str) -> Self {
                use std::sync::atomic::{AtomicU32, Ordering};
                static NEXT: AtomicU32 = AtomicU32::new(0);
                // Unique per call: three of these are alive at once, and
                // sharing a path would have each one clobber the last.
                let path = std::env::temp_dir().join(format!(
                    "indexer-venues-{}-{}.toml",
                    std::process::id(),
                    NEXT.fetch_add(1, Ordering::Relaxed)
                ));
                std::fs::write(&path, body).unwrap();
                Self(path)
            }

            pub fn path(&self) -> &Path {
                &self.0
            }
        }

        impl Drop for Temp {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.0);
            }
        }
    }
}
