//! `config/<cluster>/raffles.toml` → where the raffles program lives on one
//! cluster (PIG-134).
//!
//! The PiggyGang Raffles program is an on-chain address like any other, so it
//! is written down in `config/` and nowhere else — not in Rust, SQL or tests,
//! and not in an env var, which is where a program id usually ends up. One
//! file per cluster, beside that cluster's `collections.toml`, holding the two
//! facts the raffles pipeline is built on:
//!
//! * the **program id** — what gets subscribed to and swept for signatures;
//! * the **deploy slot** — the slot of the program's *first* deployment, the
//!   floor a cold-start replay walks back to.
//!
//! **A cluster without the program says so.** `deployed = false` is a
//! statement, and the two fields are then forbidden; `deployed = true` needs
//! both. A half-described program is an error rather than a quiet "off", so
//! the file can exist — and be checked — before the program does, and nothing
//! has to guess what a missing key meant.
//!
//! Like [`crate::seed`], a file names its cluster and is read only for that
//! one. It lives in this crate because the two files sit side by side and
//! share that rule, which is also why the cluster arrives as a `&str`: this
//! crate does not depend on `indexer-config`.
//!
//! Nothing reads it at run time yet — its readers arrive with the raffles
//! pipeline. The loader is here first so that the committed files are checked
//! from the day they exist.

use std::path::Path;

use anyhow::{bail, ensure, Context};
use serde::Deserialize;

use crate::seed::{ensure_cluster, is_pubkey};

/// The raffles program on one cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RafflesProgram {
    /// The program's address.
    pub program_id: String,
    /// The slot of the program's first deployment on this cluster: the slot of
    /// the oldest transaction in the program address's history. Not the "last
    /// deployed" slot the program account reports, which moves with every
    /// upgrade — nothing the program did is older than this one.
    pub deploy_slot: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RafflesFile {
    version: u32,
    /// The cluster this file describes, in the `SOLANA_CLUSTER` spelling.
    cluster: String,
    /// Required, never inferred from which of the fields below are present.
    deployed: bool,
    #[serde(default)]
    program_id: Option<String>,
    #[serde(default)]
    deploy_slot: Option<u64>,
}

/// Reads one cluster's `raffles.toml`.
///
/// `Ok(None)` is the file saying the program is not deployed on this cluster —
/// a present, valid answer. A missing file is an error, and so is one that
/// names another cluster than `cluster`.
pub fn load(path: &Path, cluster: &str) -> anyhow::Result<Option<RafflesProgram>> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    parse(&text, path, cluster)
}

fn parse(text: &str, path: &Path, cluster: &str) -> anyhow::Result<Option<RafflesProgram>> {
    let file: RafflesFile =
        toml::from_str(text).with_context(|| format!("parsing {}", path.display()))?;
    ensure!(
        file.version == 1,
        "{}: unsupported version {} (expected 1)",
        path.display(),
        file.version
    );
    ensure_cluster(path, "raffles program config", &file.cluster, cluster)?;

    match (file.deployed, file.program_id, file.deploy_slot) {
        (false, None, None) => Ok(None),
        (false, _, _) => bail!(
            "{}: deployed = false, but program_id or deploy_slot is set — say \
             `deployed = true` or remove them; a half-described program is read as neither",
            path.display()
        ),
        (true, Some(program_id), Some(deploy_slot)) => {
            ensure!(
                is_pubkey(&program_id),
                "{}: program_id is not a base58 32-byte pubkey: {program_id}",
                path.display()
            );
            ensure!(
                deploy_slot > 0,
                "{}: deploy_slot is the slot of the program's first deployment, never 0",
                path.display()
            );
            Ok(Some(RafflesProgram {
                program_id,
                deploy_slot,
            }))
        }
        (true, _, _) => bail!(
            "{}: deployed = true needs both program_id and deploy_slot",
            path.display()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The file committed for `cluster`: `config/<cluster>/raffles.toml`.
    fn committed_config(cluster: &str) -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../config")
            .join(cluster)
            .join("raffles.toml")
    }

    /// Obviously synthetic 32-byte keys (never a real on-chain address).
    fn synthetic_pk(seed: u8) -> String {
        bs58::encode([seed; 32]).into_string()
    }

    fn parse_for(cluster: &str, body: &str) -> anyhow::Result<Option<RafflesProgram>> {
        parse(body, Path::new("raffles.toml"), cluster)
    }

    /// A broken committed file must fail plain `cargo test`, no DB needed.
    ///
    /// Both clusters say "not deployed" today, and both are pinned on purpose:
    /// a file that starts naming a program should change this test in the same
    /// commit. When one does, assert `is_some()` and a non-zero slot here — the
    /// address itself stays in `config/`, never in a test.
    #[test]
    fn the_committed_files_are_valid_and_say_not_deployed() {
        for cluster in ["mainnet-beta", "devnet"] {
            let program = load(&committed_config(cluster), cluster)
                .unwrap_or_else(|e| panic!("config/{cluster}/raffles.toml: {e:#}"));
            assert_eq!(
                program, None,
                "{cluster} has the raffles program now: update this test with it"
            );
        }
    }

    #[test]
    fn a_file_is_read_only_for_its_own_cluster() {
        for (file, cluster) in [("mainnet-beta", "devnet"), ("devnet", "mainnet-beta")] {
            let error = load(&committed_config(file), cluster)
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(&format!("`{file}`")) && error.contains(&format!("`{cluster}`")),
                "the refusal must name both clusters: {error}"
            );
        }
    }

    #[test]
    fn a_deployed_program_carries_its_id_and_its_deploy_slot() {
        let id = synthetic_pk(7);
        let program = parse_for(
            "devnet",
            &format!(
                "version = 1\ncluster = \"devnet\"\ndeployed = true\n\
                 program_id = \"{id}\"\ndeploy_slot = 412345678\n"
            ),
        )
        .unwrap();
        assert_eq!(
            program,
            Some(RafflesProgram {
                program_id: id,
                deploy_slot: 412_345_678,
            })
        );
    }

    /// The two states are spelled out and cross-checked, so neither a
    /// forgotten flag nor a forgotten field can pass as the other state.
    #[test]
    fn a_half_described_program_is_an_error_not_an_off_switch() {
        let id = synthetic_pk(7);
        let header = "version = 1\ncluster = \"devnet\"\n";

        // Filled in, but the flag was never flipped: must not read as "off".
        let unflipped = parse_for(
            "devnet",
            &format!("{header}deployed = false\nprogram_id = \"{id}\"\ndeploy_slot = 5\n"),
        );
        assert!(
            format!("{:#}", unflipped.unwrap_err()).contains("deployed = false"),
            "an id beside deployed = false is refused"
        );
        let slot_only = parse_for(
            "devnet",
            &format!("{header}deployed = false\ndeploy_slot = 5\n"),
        );
        assert!(slot_only.is_err(), "so is a lone deploy slot");

        // Flipped, but a field is missing: there is no partial program.
        for body in [
            format!("{header}deployed = true\n"),
            format!("{header}deployed = true\nprogram_id = \"{id}\"\n"),
            format!("{header}deployed = true\ndeploy_slot = 5\n"),
        ] {
            let error = format!("{:#}", parse_for("devnet", &body).unwrap_err());
            assert!(error.contains("needs both"), "{error}");
        }

        // And saying nothing is not saying "not deployed".
        let silent = parse_for("devnet", header).unwrap_err();
        assert!(format!("{silent:#}").contains("deployed"), "{silent:#}");
    }

    #[test]
    fn typos_fail_loudly() {
        let id = synthetic_pk(7);
        let deployed = |program_id: &str, slot: &str| {
            format!(
                "version = 1\ncluster = \"devnet\"\ndeployed = true\n\
                 program_id = \"{program_id}\"\ndeploy_slot = {slot}\n"
            )
        };

        let bad_id = parse_for("devnet", &deployed("not-base58-0OIl", "5")).unwrap_err();
        assert!(format!("{bad_id:#}").contains("not a base58"), "{bad_id:#}");

        // 31 bytes is base58, and still not an address.
        let short = bs58::encode([7u8; 31]).into_string();
        assert!(parse_for("devnet", &deployed(&short, "5")).is_err());

        assert!(
            parse_for("devnet", &deployed(&id, "0")).is_err(),
            "slot 0 is a forgotten value, not a deployment"
        );
        assert!(
            parse_for("devnet", &deployed(&id, "-1")).is_err(),
            "a slot is unsigned"
        );

        let unknown_key = parse_for(
            "devnet",
            "version = 1\ncluster = \"devnet\"\ndeployed = false\ndeploy_slto = 5\n",
        )
        .unwrap_err();
        assert!(
            format!("{unknown_key:#}").contains("deploy_slto"),
            "{unknown_key:#}"
        );

        let version = parse_for(
            "devnet",
            "version = 2\ncluster = \"devnet\"\ndeployed = false\n",
        )
        .unwrap_err();
        assert!(
            format!("{version:#}").contains("unsupported version"),
            "{version:#}"
        );
    }
}
