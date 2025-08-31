use anyhow::{Context, Result, bail};
use std::path::PathBuf;

use crate::config::Config;
use crate::utils::naming::parse_archive_name;
use crate::utils::process::{CmdSpec, Pipeline, Runner, StdioSpec};

use super::{PbsSnapshot, Provider, RestoreItem};

pub struct ZfsRestore<'a, R: Runner> {
    dest_root: String,
    snapshot: Option<&'a PbsSnapshot>,
    runner: &'a R,
}

impl<'a, R: Runner> ZfsRestore<'a, R> {
    pub fn new(cfg: &Config, snapshot: Option<&'a PbsSnapshot>, runner: &'a R) -> Self {
        let z = cfg
            .zfs
            .as_ref()
            .expect("[zfs] missing in config (restore disabled)");

        let dest_root = z
            .restore
            .as_ref()
            .map(|r| r.dest_root.clone())
            .expect("[zfs.restore] missing dest_root");

        Self {
            dest_root,
            snapshot,
            runner,
        }
    }

    fn resolve_dataset_target(&self, archive: &str) -> Result<PathBuf> {
        let (provider, leaf, _id) = parse_archive_name(archive)?;
        if provider != "zfs" {
            bail!("not a zfs archive: {archive}");
        }

        let dataset = format!("{}/{}", self.dest_root, leaf);

        // check dataset existence
        let cmd_list = CmdSpec::new("zfs")
            .args(["list", "-H", "-o", "name", &dataset])
            .stdout(StdioSpec::Null)
            .stderr(StdioSpec::Null);
        self.runner
            .run(&Pipeline::new().cmd(cmd_list))
            .with_context(|| format!("zfs list {dataset}"))?;

        // resolve mountpoint
        let cmd_get = CmdSpec::new("zfs")
            .args(["get", "-H", "-o", "value", "mountpoint", &dataset])
            .stdout(StdioSpec::Pipe)
            .stderr(StdioSpec::Null);
        let mountpoint = self
            .runner
            .run_capture(&Pipeline::new().cmd(cmd_get))
            .with_context(|| format!("zfs get mountpoint {dataset}"))?
            .trim()
            .to_string();

        let target = if mountpoint == "-" || mountpoint == "none" {
            PathBuf::from(format!("/dev/zvol/{dataset}"))
        } else {
            PathBuf::from(format!("{mountpoint}/{leaf}"))
        };

        Ok(target)
    }
}

impl<'a, R: Runner> Provider for ZfsRestore<'a, R> {
    fn name(&self) -> &'static str {
        "zfs-restore"
    }

    fn collect_restore(
        &mut self,
        archive: Option<&str>,
        all: bool,
        _force: bool,
    ) -> Result<Vec<RestoreItem>> {
        let mut out = Vec::new();

        if let Some(a) = archive {
            if !a.starts_with("zfs_") {
                return Ok(out);
            }
            if let Some(snap) = self.snapshot {
                let file = snap
                    .files
                    .iter()
                    .find(|f| f.filename == a)
                    .ok_or_else(|| anyhow::anyhow!("archive {a} not found in snapshot"))?;

                let target = self.resolve_dataset_target(&file.filename)?;
                out.push(RestoreItem {
                    archive: file.filename.clone(),
                    target,
                    label: format!("zfs:{}", file.filename),
                });
            } else {
                bail!("no snapshot context for archive {a}");
            }
        } else if all {
            if let Some(snap) = self.snapshot {
                for f in &snap.files {
                    if f.filename.starts_with("zfs_") {
                        let target = self.resolve_dataset_target(&f.filename)?;
                        out.push(RestoreItem {
                            archive: f.filename.clone(),
                            target,
                            label: format!("zfs:{}", f.filename),
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
            .filter(|f| f.filename.starts_with("zfs_"))
            .map(|f| f.filename.clone())
            .collect()
    }
}
