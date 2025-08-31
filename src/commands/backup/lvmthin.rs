use super::{Provider, Volume};
use anyhow::{Context, Result};
use serde::Deserialize;
use std::{collections::HashSet, path::PathBuf};
use tracing as log;

use crate::utils::process::{CmdSpec, Pipeline, Runner, StdioSpec};
use crate::{
    config::{Config, Pbs},
    utils::{
        bins::ensure_bins, dev::wait_for_block, ids::lvmthin_short8, naming::create_archive_name,
        time::current_epoch,
    },
};

enum Reject<'a> {
    NotThin,
    VgNotAllowed(&'a str),
    PvDenied,
}

const CLONE_SUFFIX: &str = "pvtools";

pub struct LvmThinProvider<'a, R: Runner> {
    vgs_set: HashSet<String>,
    pbs: &'a Pbs,
    run_ts: u64,
    cleanup: Cleanup<'a, R>,
    runner: &'a R,
}

impl<'a, R: Runner> LvmThinProvider<'a, R> {
    pub fn new(cfg: &'a Config, runner: &'a R) -> Self {
        let l = cfg
            .lvmthin
            .as_ref()
            .expect("[lvmthin] missing in config (provider disabled)");

        Self {
            vgs_set: l.vgs.iter().map(|s| s.trim().to_string()).collect(),
            pbs: &cfg.pbs,
            run_ts: current_epoch(),
            cleanup: Cleanup::new(runner),
            runner,
        }
    }

    fn accept_lv<'b>(&self, lv: &'b LvRow) -> std::result::Result<(), Reject<'b>> {
        if !matches!(lv.segtype.as_deref(), Some("thin")) {
            return Err(Reject::NotThin);
        }
        if !self.vgs_set.contains(&lv.vg_name) {
            return Err(Reject::VgNotAllowed(&lv.vg_name));
        }
        if !self.pbs.pv_allows(&lv.lv_name) {
            return Err(Reject::PvDenied);
        }
        Ok(())
    }
}

impl<'a, R: Runner> Provider for LvmThinProvider<'a, R> {
    fn name(&self) -> &'static str {
        "lvmthin"
    }

    fn collect(&mut self, dry_run: bool) -> Result<Vec<Volume>> {
        ensure_bins(["lvs", "lvcreate", "lvchange", "lvremove"])?;

        let mut out = Vec::<Volume>::new();
        let rows = list_lvmthin(self.runner).context("run lvs and parse JSON")?;

        for lv in rows {
            match self.accept_lv(&lv) {
                Ok(()) => {
                    let name = format!("{}/{}", lv.vg_name, lv.lv_name);
                    let leaf = &lv.lv_name;
                    let id8 = lvmthin_short8(&lv.vg_name, &lv.lv_name)
                        .with_context(|| format!("get lv_uuid short8 for {name}"))?;
                    let archive = create_archive_name("lvmthin", leaf, &id8)?;

                    let (device, ops, snap_fq) =
                        plan_lv(&lv.vg_name, &lv.lv_name, CLONE_SUFFIX, self.run_ts);

                    if dry_run {
                        for op in &ops {
                            log::info!("[backup] DRY-RUN: {}", op.render());
                        }
                    } else {
                        for op in &ops {
                            self.runner
                                .run(&Pipeline::new().cmd(op.clone()))
                                .with_context(|| format!("lvmthin op on {name}"))?;
                        }
                        wait_for_block(&device)
                            .with_context(|| format!("wait for {}", device.display()))?;
                        self.cleanup.add(snap_fq);
                    }

                    out.push(Volume {
                        archive,
                        device,
                        label: format!("lvmthin:{name}"),
                        map_src: format!("/dev/{name}"),
                    });
                }
                Err(Reject::NotThin) => log::trace!("skip {}: segtype != thin", lv.lv_name),
                Err(Reject::VgNotAllowed(vg)) => {
                    log::trace!("skip {}: vg '{}' not allowed", lv.lv_name, vg)
                }
                Err(Reject::PvDenied) => log::trace!("skip {}: pv_allows=false", lv.lv_name),
            }
        }

        if out.is_empty() {
            log::debug!("lvmthin: no candidate volumes");
        }

        Ok(out)
    }
}

struct Cleanup<'a, R: Runner> {
    snaps: Vec<String>,
    runner: &'a R,
}

impl<'a, R: Runner> Cleanup<'a, R> {
    fn new(runner: &'a R) -> Self {
        Self {
            snaps: Vec::new(),
            runner,
        }
    }

    fn add(&mut self, snap_fq: String) {
        self.snaps.push(snap_fq);
    }
}

impl<'a, R: Runner> Drop for Cleanup<'a, R> {
    fn drop(&mut self) {
        for s in self.snaps.drain(..) {
            let cmd = CmdSpec::new("lvremove").args(["-f", &s]);
            let _ = self.runner.run(&Pipeline::new().cmd(cmd));
        }
    }
}

fn plan_lv(vg: &str, lv: &str, suffix: &str, ts: u64) -> (PathBuf, Vec<CmdSpec>, String) {
    let snap = format!("{lv}-{suffix}-{ts}");
    let snap_fq = format!("{vg}/{snap}");
    let dev_path = PathBuf::from(format!("/dev/{snap_fq}"));

    let ops = vec![
        CmdSpec::new("lvcreate").args(["-s", "-n", &snap, &format!("{vg}/{lv}")]),
        CmdSpec::new("lvchange").args(["-K", "-ay", &snap_fq]),
    ];

    (dev_path, ops, snap_fq)
}

#[derive(Deserialize)]
struct LvsJson {
    report: Vec<Report>,
}
#[derive(Deserialize)]
struct Report {
    lv: Vec<LvRow>,
}
#[derive(Deserialize)]
struct LvRow {
    lv_name: String,
    vg_name: String,
    #[serde(default)]
    segtype: Option<String>,
}

fn list_lvmthin(runner: &dyn Runner) -> Result<Vec<LvRow>> {
    let cmd = CmdSpec::new("lvs")
        .args([
            "--reportformat",
            "json",
            "--units",
            "b",
            "-o",
            "lv_name,vg_name,segtype",
        ])
        .stdout(StdioSpec::Pipe);

    let out = runner
        .run_capture(&Pipeline::new().cmd(cmd))
        .context("run lvs")?;

    let json: LvsJson = serde_json::from_str(&out).context("parse lvs json (full list)")?;
    Ok(json.report.into_iter().flat_map(|r| r.lv).collect())
}
