use anyhow::{Context, Result, bail};
use std::path::PathBuf;

use crate::config::Config;
use crate::utils::naming::parse_archive_name;
use crate::utils::process::{CmdSpec, Pipeline, Runner, StdioSpec};

use super::{PbsSnapshot, Provider, RestoreItem};

pub struct LvmthinRestore<'a, R: Runner> {
    vg: String,
    snapshot: Option<&'a PbsSnapshot>,
    runner: &'a R,
}

impl<'a, R: Runner> LvmthinRestore<'a, R> {
    pub fn new(cfg: &Config, snapshot: Option<&'a PbsSnapshot>, runner: &'a R) -> Self {
        let l = cfg
            .lvmthin
            .as_ref()
            .expect("[lvmthin] missing in config (restore disabled)");

        let vg = l
            .restore
            .as_ref()
            .map(|r| r.vg.clone())
            .expect("[lvmthin.restore] missing vg");

        Self {
            vg,
            snapshot,
            runner,
        }
    }

    fn resolve_lv_target(&self, archive: &str) -> Result<PathBuf> {
        let (provider, leaf, _id) = parse_archive_name(archive)?;
        if provider != "lvmthin" {
            bail!("not an lvmthin archive: {archive}");
        }

        let lv_path = format!("/dev/{}/{}", self.vg, leaf);

        let cmd = CmdSpec::new("lvs")
            .args(["--noheadings", "-o", "lv_name", &lv_path])
            .stdout(StdioSpec::Null)
            .stderr(StdioSpec::Null);

        self.runner
            .run(&Pipeline::new().cmd(cmd))
            .with_context(|| format!("lvs check {lv_path}"))?;

        Ok(PathBuf::from(lv_path))
    }
}

impl<'a, R: Runner> Provider for LvmthinRestore<'a, R> {
    fn name(&self) -> &'static str {
        "lvmthin"
    }

    fn collect_restore(
        &mut self,
        archive: Option<&str>,
        all: bool,
        _force: bool,
    ) -> Result<Vec<RestoreItem>> {
        let mut out = Vec::new();

        if let Some(a) = archive {
            if !a.starts_with("lvmthin_") {
                return Ok(out);
            }
            if let Some(snap) = self.snapshot {
                let file = snap
                    .files
                    .iter()
                    .find(|f| f.filename == a)
                    .ok_or_else(|| anyhow::anyhow!("archive {a} not found in snapshot"))?;

                let target = self.resolve_lv_target(&file.filename)?;
                out.push(RestoreItem {
                    archive: file.filename.clone(),
                    target,
                    label: format!("lvmthin:{}", file.filename),
                });
            } else {
                bail!("no snapshot context for archive {a}");
            }
        } else if all {
            if let Some(snap) = self.snapshot {
                for f in &snap.files {
                    if f.filename.starts_with("lvmthin_") {
                        let target = self.resolve_lv_target(&f.filename)?;
                        out.push(RestoreItem {
                            archive: f.filename.clone(),
                            target,
                            label: format!("lvmthin:{}", f.filename),
                        });
                    }
                }
            } else {
                bail!("no snapshot context provided for restore-all");
            }
        }

        Ok(out)
    }

    fn list_archives(&self, snap: &PbsSnapshot) -> Vec<String> {
        snap.files
            .iter()
            .filter(|f| f.filename.starts_with("lvmthin_"))
            .map(|f| f.filename.clone())
            .collect()
    }
}
