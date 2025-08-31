use super::{Provider, Volume};
use anyhow::{Context, Result, bail};
use std::{path::PathBuf, process::Command};
use tracing as log;

use crate::{
    config::{Config, Pbs},
    utils::{
        bins::ensure_bins, cmd::cmd_ok, dev::wait_for_block, ids::zfs_guids,
        naming::create_archive_name, path::dataset_leaf, time::current_epoch,
    },
};

const DEV_PREFIX: &str = "/dev/zvol/";
const CLONE_SUFFIX: &str = "pvtools";

enum Reject<'a> {
    NotBase(&'a str),
    PvDenied(&'a str),
}

pub struct ZfsProvider<'a> {
    pools: &'a [String],
    pbs: &'a Pbs,
    run_ts: u64,
    cleanup: Cleanup,
}

impl<'a> ZfsProvider<'a> {
    pub fn new(cfg: &'a Config) -> Self {
        let z = cfg.zfs.as_ref().expect("[zfs] missing");

        Self {
            pools: &z.pools,
            pbs: &cfg.pbs,
            run_ts: current_epoch(),
            cleanup: Cleanup::default(),
        }
    }

    #[inline]
    fn accept_ds<'b>(&self, name: &'b str, origin: &'b str) -> std::result::Result<(), Reject<'b>> {
        if origin != "-" {
            return Err(Reject::NotBase(origin));
        }
        let leaf = dataset_leaf(name);
        if !self.pbs.pv_allows(leaf) {
            return Err(Reject::PvDenied(leaf));
        }
        Ok(())
    }
}

impl<'a> Provider for ZfsProvider<'a> {
    fn name(&self) -> &'static str {
        "zfs"
    }

    fn collect(&mut self, dry_run: bool) -> Result<Vec<Volume>> {
        ensure_bins(["zfs"])?;
        let mut out = Vec::<Volume>::new();

        for pool in self.pools {
            // zfs list -H -t volume -o name,origin -r <pool>
            let out_txt = Command::new("zfs")
                .args([
                    "list",
                    "-H",
                    "-t",
                    "volume",
                    "-o",
                    "name,origin",
                    "-r",
                    pool,
                ])
                .output()
                .with_context(|| format!("zfs list for pool {pool}"))?;

            if !out_txt.status.success() {
                bail!("zfs list failed for pool {pool}");
            }

            let s = String::from_utf8_lossy(&out_txt.stdout);
            let guid_map = zfs_guids(pool)?;
            for line in s.lines() {
                let mut it = line.split_whitespace();
                let name = match it.next() {
                    Some(x) => x,
                    None => continue,
                };
                let origin = match it.next() {
                    Some(x) => x,
                    None => continue,
                };

                match self.accept_ds(name, origin) {
                    Ok(()) => {
                        let (device, ops, snap, clone) = plan_zvol(name, CLONE_SUFFIX, self.run_ts);

                        if dry_run {
                            for op in &ops {
                                log::info!("[backup] DRY-RUN: {} {}", op.bin, op.args.join(" "));
                            }
                        } else {
                            for op in &ops {
                                cmd_ok(op.bin, &op.args)?;
                            }
                            self.cleanup.add(snap, clone.clone());
                            wait_for_block(&device)?;
                        }

                        let leaf = dataset_leaf(name);
                        let id8 = guid_map.get(name).ok_or_else(|| {
                            anyhow::anyhow!("guid not found for dataset {}", name)
                        })?;
                        let archive = create_archive_name("zfs", leaf, id8)?;

                        log::trace!("leaf={leaf:?}, archive={archive:?}");

                        out.push(Volume {
                            archive,
                            device,
                            label: format!("zfs:{name}"),
                            map_src: format!("/{name}"),
                        });
                    }
                    Err(Reject::NotBase(orig)) => {
                        log::trace!("skip {}: origin != '-' (origin='{}')", name, orig)
                    }
                    Err(Reject::PvDenied(leaf)) => {
                        log::trace!("skip {}: pv_allows(false) for leaf '{}'", name, leaf)
                    }
                }
            }
        }

        if out.is_empty() {
            log::debug!("zfs: no candidate volumes");
        }

        Ok(out)
    }
}

#[derive(Default)]
struct Cleanup {
    snaps: Vec<String>,
    clones: Vec<String>,
}

impl Cleanup {
    fn add(&mut self, snap: String, clone: String) {
        self.snaps.push(snap);
        self.clones.push(clone);
    }
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        for c in self.clones.drain(..) {
            let _ = Command::new("zfs").args(["destroy", "-r", &c]).status();
        }
        for s in self.snaps.drain(..) {
            let _ = Command::new("zfs").args(["destroy", "-r", &s]).status();
        }
    }
}

struct ZfsOp {
    bin: &'static str,
    args: Vec<String>,
}

fn plan_zvol(ds: &str, suffix: &str, ts: u64) -> (PathBuf, Vec<ZfsOp>, String, String) {
    let snap = format!("{ds}@{suffix}-{ts}");
    let clone = format!("{ds}-{suffix}-{ts}");

    let ops = vec![
        ZfsOp {
            bin: "zfs",
            args: vec!["snapshot".into(), snap.clone()],
        },
        ZfsOp {
            bin: "zfs",
            args: vec![
                "clone".into(),
                "-o".into(),
                "readonly=on".into(),
                "-o".into(),
                "volmode=dev".into(),
                snap.clone(),
                clone.clone(),
            ],
        },
    ];

    let dev = PathBuf::from(format!("{DEV_PREFIX}{clone}"));
    (dev, ops, snap, clone)
}
