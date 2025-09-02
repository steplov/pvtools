use super::super::types::{Provider, Volume};
use anyhow::{Context, Result};
use serde::Deserialize;
use std::{collections::HashSet, path::PathBuf, sync::Arc};
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

#[derive(Debug, Clone)]
struct LvmMeta {
    vg: String,
    lv: String,
    run_ts: u64,
}

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

    fn discover(&self) -> Result<Vec<Volume>> {
        ensure_bins(["lvs", "lvcreate", "lvchange", "lvremove"])?;

        let mut out = Vec::<Volume>::new();
        let rows = list_lvmthin(self.runner).context("run lvs and parse JSON")?;

        for lv in rows {
            match self.accept_lv(&lv) {
                Ok(()) => {
                    let name = format!("{}/{}", lv.vg_name, lv.lv_name);
                    let id8 = lvmthin_short8(&lv.vg_name, &lv.lv_name, self.runner)
                        .with_context(|| format!("get lv_uuid short8 for {name}"))?;
                    let archive = create_archive_name("lvmthin", &lv.lv_name, &id8)?;

                    let names =
                        build_lvm_names(&lv.vg_name, &lv.lv_name, CLONE_SUFFIX, self.run_ts);

                    out.push(Volume {
                        archive,
                        device: names.device.clone(),
                        label: format!("lvmthin:{name}"),
                        map_src: format!("/dev/{name}"),
                        meta: Some(Arc::new(LvmMeta {
                            vg: lv.vg_name.clone(),
                            lv: lv.lv_name.clone(),
                            run_ts: self.run_ts,
                        })),
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

    fn prepare(&mut self, volumes: &[Volume], dry_run: bool) -> Result<()> {
        ensure_bins(["lvs", "lvcreate", "lvchange", "lvremove"])?;
        for v in volumes {
            let meta = match v.meta::<LvmMeta>() {
                Some(m) => m,
                None => continue,
            };

            let names = build_lvm_names(&meta.vg, &meta.lv, CLONE_SUFFIX, meta.run_ts);

            let ops = vec![
                CmdSpec::new("lvcreate").args([
                    "-s",
                    "-n",
                    &names.snap,
                    &format!("{}/{}", meta.vg, meta.lv),
                ]),
                CmdSpec::new("lvchange").args(["-K", "-ay", &names.snap_fq]),
            ];

            if dry_run {
                for op in &ops {
                    log::info!("[backup] DRY-RUN: {}", op.render());
                }
            } else {
                for op in &ops {
                    self.runner
                        .run(&Pipeline::new().cmd(op.clone()))
                        .with_context(|| format!("lvmthin op on {}/{}", meta.vg, meta.lv))?;
                }
                wait_for_block(&names.device, self.runner)
                    .with_context(|| format!("wait for {}", names.device.display()))?;
                self.cleanup.add(names.snap_fq);
            }
        }

        Ok(())
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
            if let Err(e) = self.runner.run(&Pipeline::new().cmd(cmd)) {
                log::warn!("[cleanup] failed to remove LV snapshot {s}: {e}");
            }
        }
    }
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

#[derive(Debug, Clone)]
struct LvmNames {
    snap: String,
    snap_fq: String,
    device: PathBuf,
}

#[inline]
fn build_lvm_names(vg: &str, lv: &str, suffix: &str, ts: u64) -> LvmNames {
    let snap = format!("{lv}-{suffix}-{ts}");
    let snap_fq = format!("{vg}/{snap}");
    let device = PathBuf::from(format!("/dev/{snap_fq}"));

    LvmNames {
        snap,
        snap_fq,
        device,
    }
}
