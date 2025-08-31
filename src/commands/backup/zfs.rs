use super::{Provider, Volume};
use anyhow::{Context, Result};
use std::path::PathBuf;
use tracing as log;

use crate::utils::process::{CmdSpec, Pipeline, Runner, StdioSpec};
use crate::{
    config::{Config, Pbs},
    utils::{
        bins::ensure_bins, dev::wait_for_block, ids::zfs_guids, naming::create_archive_name,
        path::dataset_leaf, time::current_epoch,
    },
};

const DEV_PREFIX: &str = "/dev/zvol/";
const CLONE_SUFFIX: &str = "pvtools";

enum Reject<'a> {
    NotBase(&'a str),
    PvDenied(&'a str),
}

pub struct ZfsProvider<'a, R: Runner> {
    pools: &'a [String],
    pbs: &'a Pbs,
    run_ts: u64,
    cleanup: Cleanup<'a, R>,
    runner: &'a R,
}

impl<'a, R: Runner> ZfsProvider<'a, R> {
    pub fn new(cfg: &'a Config, runner: &'a R) -> Self {
        let z = cfg.zfs.as_ref().expect("[zfs] missing");

        Self {
            pools: &z.pools,
            pbs: &cfg.pbs,
            run_ts: current_epoch(),
            cleanup: Cleanup::new(runner),
            runner,
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

impl<'a, R: Runner> Provider for ZfsProvider<'a, R> {
    fn name(&self) -> &'static str {
        "zfs"
    }

    fn collect(&mut self, dry_run: bool) -> Result<Vec<Volume>> {
        ensure_bins(["zfs"])?;
        let mut out = Vec::<Volume>::new();

        for pool in self.pools {
            // zfs list -H -t volume -o name,origin -r <pool>
            let cmd = CmdSpec::new("zfs")
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
                .stdout(StdioSpec::Pipe);

            let out_txt = self
                .runner
                .run_capture(&Pipeline::new().cmd(cmd))
                .with_context(|| format!("zfs list for pool {pool}"))?;

            let guid_map = zfs_guids(pool)?;
            for line in out_txt.lines() {
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
                                log::info!("[backup] DRY-RUN: {}", op.render());
                            }
                        } else {
                            for op in &ops {
                                self.runner
                                    .run(&Pipeline::new().cmd(op.clone()))
                                    .with_context(|| format!("zfs op on {name}"))?;
                            }
                            self.cleanup.add_many([snap, clone.clone()]);
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
struct Cleanup<'a, R: Runner> {
    tasks: Vec<CmdSpec>,
    runner: Option<&'a R>,
}

impl<'a, R: Runner> Cleanup<'a, R> {
    fn new(runner: &'a R) -> Self {
        Self {
            tasks: Vec::new(),
            runner: Some(runner),
        }
    }

    fn add_many<I: IntoIterator<Item = String>>(&mut self, snaps: I) {
        for s in snaps {
            self.tasks
                .push(CmdSpec::new("zfs").args(["destroy", "-r", &s]));
        }
    }
}

impl<'a, R: Runner> Drop for Cleanup<'a, R> {
    fn drop(&mut self) {
        if let Some(r) = self.runner {
            for cmd in self.tasks.drain(..) {
                if let Err(e) = r.run(&Pipeline::new().cmd(cmd)) {
                    log::warn!("cleanup failed: {e}");
                }
            }
        }
    }
}

fn plan_zvol(ds: &str, suffix: &str, ts: u64) -> (PathBuf, Vec<CmdSpec>, String, String) {
    let snap = format!("{ds}@{suffix}-{ts}");
    let clone = format!("{ds}-{suffix}-{ts}");

    let ops = vec![
        CmdSpec::new("zfs").args(["snapshot", &snap]),
        CmdSpec::new("zfs").args([
            "clone",
            "-o",
            "readonly=on",
            "-o",
            "volmode=dev",
            &snap,
            &clone,
        ]),
    ];

    let dev = PathBuf::from(format!("{DEV_PREFIX}{clone}"));
    (dev, ops, snap, clone)
}
