use std::{collections::HashSet, path::PathBuf, sync::Arc};

use anyhow::{Context, Result, anyhow};
use tracing as log;

use crate::{
    commands::backup::providers::Provider,
    config::{Config, Pbs},
    tooling::{BlockPort, LvmPort, PveshPort, lvm::LvInfo, pvesh::Storage},
    utils::{exec_policy, naming::create_archive_name, time::current_epoch},
    volume::Volume,
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

pub struct LvmThinProvider<'a> {
    vgs_set: HashSet<String>,
    pbs: &'a Pbs,
    run_ts: u64,
    cleanup: Cleanup,
    lvm: Arc<dyn LvmPort>,
    block: Arc<dyn BlockPort>,
    pvesh: Arc<dyn PveshPort>,
}

impl<'a> LvmThinProvider<'a> {
    pub fn new(
        cfg: &'a Config,
        lvm: Arc<dyn LvmPort>,
        block: Arc<dyn BlockPort>,
        pvesh: Arc<dyn PveshPort>,
    ) -> Self {
        let l = cfg
            .lvmthin
            .as_ref()
            .expect("[lvmthin] missing in config (provider disabled)");

        Self {
            vgs_set: l.vgs.iter().map(|s| s.trim().to_string()).collect(),
            pbs: &cfg.pbs,
            run_ts: current_epoch(),
            cleanup: Cleanup::new(lvm.clone()),
            lvm,
            block,
            pvesh,
        }
    }

    fn accept_lv<'b>(&self, lv: &'b LvInfo) -> std::result::Result<(), Reject<'b>> {
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

impl<'a> Provider for LvmThinProvider<'a> {
    fn name(&self) -> &'static str {
        "lvmthin"
    }

    fn discover(&self) -> Result<Vec<Volume>> {
        let mut out = Vec::<Volume>::new();
        let rows = self.lvm.list_lvs().context("run lvs and parse JSON")?;
        let storages = self.pvesh.get_storage()?;

        for lv in rows {
            match self.accept_lv(&lv) {
                Ok(()) => {
                    let name = format!("{}/{}", lv.vg_name, lv.lv_name);
                    let id8 = self
                        .lvm
                        .lv_uuid_short8(&lv.vg_name, &lv.lv_name)
                        .with_context(|| format!("get lv_uuid short8 for {name}"))?;
                    let archive = create_archive_name("lvmthin", &lv.lv_name, &id8)?;

                    let names =
                        build_lvm_names(&lv.vg_name, &lv.lv_name, CLONE_SUFFIX, self.run_ts);

                    let storage_id = find_storage(&storages, &lv.vg_name)?;

                    let a = Volume {
                        storage: storage_id.to_string(),
                        disk: lv.lv_name.clone(),
                        archive: archive.clone(),
                        device: names.device.clone(),
                        meta: Some(Arc::new(LvmMeta {
                            vg: lv.vg_name.clone(),
                            lv: lv.lv_name.clone(),
                            run_ts: self.run_ts,
                        })),
                    };
                    dbg!(a);

                    out.push(Volume {
                        storage: storage_id.to_string(),
                        disk: lv.lv_name.clone(),
                        archive,
                        device: names.device.clone(),
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

    fn prepare(&mut self, volumes: &[Volume]) -> Result<()> {
        for v in volumes {
            let meta = match v.meta::<LvmMeta>() {
                Some(m) => m,
                None => continue,
            };

            let names = build_lvm_names(&meta.vg, &meta.lv, CLONE_SUFFIX, meta.run_ts);

            self.lvm
                .lvcreate_snapshot(&meta.vg, &meta.lv, &names.snap)
                .with_context(|| format!("lv snapshot on {}", &names.snap))?;
            self.lvm
                .lvchange_activate(&names.snap_fq)
                .with_context(|| format!("lv change on {}", &names.snap))?;

            if !exec_policy::is_dry_run() {
                self.block.wait_for_block(&names.device)?;
                self.cleanup.add(names.snap_fq);
            }
        }

        Ok(())
    }
}

struct Cleanup {
    snaps: Vec<String>,
    lvm: Option<Arc<dyn LvmPort>>,
}

impl Cleanup {
    fn new(lvm: Arc<dyn LvmPort>) -> Self {
        Self {
            snaps: Vec::new(),
            lvm: Some(lvm),
        }
    }

    fn add(&mut self, snap_fq: String) {
        self.snaps.push(snap_fq);
    }
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        if let Some(lvm) = &self.lvm {
            for s in self.snaps.drain(..) {
                if let Err(e) = lvm.lvremove_force(&s) {
                    log::warn!("[cleanup] lvremove -f {} failed: {e}", s);
                }
            }
        }
    }
}

#[inline]
fn find_storage<'a>(storages: &'a [Storage], vg_name: &str) -> Result<&'a str> {
    storages
        .iter()
        .find_map(|s| match *s {
            Storage::LvmThin {
                ref id,
                vgname: ref storage_name,
                ..
            } if storage_name.as_str() == vg_name => Some(id.as_str()),
            _ => None,
        })
        .ok_or_else(|| anyhow!("LVM-thin storage with vgname='{vg_name}' not found"))
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

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

    use anyhow::Result;

    use super::*;
    use crate::{
        config::{Config, LvmThin, Pbs},
        tooling::{BlockPort, LvmPort, lvm::LvInfo},
        utils::process::ProcessRunner,
    };

    struct MockLvm {
        lvs: Vec<LvInfo>,
    }

    impl LvmPort for MockLvm {
        fn list_lvs(&self) -> Result<Vec<LvInfo>> {
            Ok(self
                .lvs
                .iter()
                .map(|lv| LvInfo {
                    lv_name: lv.lv_name.clone(),
                    vg_name: lv.vg_name.clone(),
                    segtype: lv.segtype.clone(),
                })
                .collect())
        }
        fn lv_uuid_short8(&self, _vg: &str, _lv: &str) -> Result<String> {
            Ok("abcd1234".to_string())
        }
        fn lvcreate_snapshot(&self, _vg: &str, _lv: &str, _snap: &str) -> Result<String> {
            Ok("snap_path".to_string())
        }
        fn lvchange_activate(&self, _lv_fq: &str) -> Result<()> {
            Ok(())
        }
        fn lvremove_force(&self, _lv_fq: &str) -> Result<()> {
            Ok(())
        }
        fn lv_name(&self, _vg: &str, _leaf: &str) -> Result<String> {
            Ok(_leaf.to_string())
        }
    }

    struct MockBlock;
    impl BlockPort for MockBlock {
        fn wait_for_block(&self, _path: &Path) -> Result<()> {
            Ok(())
        }
        fn wait_for_block_with(
            &self,
            _dev: &Path,
            _timeout: Duration,
            _delay: Duration,
        ) -> Result<()> {
            Ok(())
        }
    }

    struct MockPveSh;
    impl PveshPort for MockPveSh {
        fn get_storage(&self) -> Result<Vec<Storage>> {
            Ok(vec![Storage::LvmThin {
                id: "local-lvm".to_string(),
                vgname: "pve".to_string(),
                thinpool: "data".to_string(),
                content: vec!["".to_string()],
            }])
        }
    }

    fn test_config() -> Config {
        Config {
            pbs: Pbs {
                repos: HashMap::new(),
                keyfile: None,
                password: None,
                ns: None,
                backup_id: "test".to_string(),
                pv_prefixes: vec!["vm-".to_string()],
                pv_exclude_re: None,
                pv_exclude_re_src: None,
            },
            zfs: None,
            lvmthin: Some(LvmThin {
                vgs: vec!["pve".to_string()],
                restore: None,
            }),
        }
    }

    #[test]
    fn build_lvm_names_correct() {
        let names = build_lvm_names("pve", "vm-123-disk", "pvtools", 1234567890);
        assert_eq!(names.snap, "vm-123-disk-pvtools-1234567890");
        assert_eq!(names.snap_fq, "pve/vm-123-disk-pvtools-1234567890");
        assert_eq!(
            names.device,
            PathBuf::from("/dev/pve/vm-123-disk-pvtools-1234567890")
        );
    }

    #[test]
    fn accept_lv_rejects_non_thin() {
        let cfg = test_config();
        let lvm = Arc::new(MockLvm { lvs: vec![] });
        let block = Arc::new(MockBlock);
        let pvesh = Arc::new(MockPveSh);
        let provider = LvmThinProvider::new(&cfg, lvm, block, pvesh);

        let lv = LvInfo {
            lv_name: "vm-123.raw".to_string(),
            vg_name: "pve".to_string(),
            segtype: Some("linear".to_string()),
        };

        let result = provider.accept_lv(&lv);
        assert!(matches!(result, Err(Reject::NotThin)));
    }

    #[test]
    fn accept_lv_rejects_wrong_vg() {
        let cfg = test_config();
        let lvm = Arc::new(MockLvm { lvs: vec![] });
        let block = Arc::new(MockBlock);
        let pvesh = Arc::new(MockPveSh);
        let provider = LvmThinProvider::new(&cfg, lvm, block, pvesh);

        let lv = LvInfo {
            lv_name: "vm-123.raw".to_string(),
            vg_name: "other".to_string(),
            segtype: Some("thin".to_string()),
        };

        let result = provider.accept_lv(&lv);
        assert!(matches!(result, Err(Reject::VgNotAllowed(_))));
    }

    #[test]
    fn accept_lv_rejects_non_pv() {
        let cfg = test_config();
        let lvm = Arc::new(MockLvm { lvs: vec![] });
        let block = Arc::new(MockBlock);
        let pvesh = Arc::new(MockPveSh);
        let provider = LvmThinProvider::new(&cfg, lvm, block, pvesh);

        let lv = LvInfo {
            lv_name: "other-123".to_string(),
            vg_name: "pve".to_string(),
            segtype: Some("thin".to_string()),
        };

        let result = provider.accept_lv(&lv);
        assert!(matches!(result, Err(Reject::PvDenied)));
    }

    #[test]
    fn accept_lv_allows_valid() {
        let cfg = test_config();
        let lvm = Arc::new(MockLvm { lvs: vec![] });
        let block = Arc::new(MockBlock);
        let pvesh = Arc::new(MockPveSh);
        let provider = LvmThinProvider::new(&cfg, lvm, block, pvesh);

        let lv = LvInfo {
            lv_name: "vm-123.raw".to_string(),
            vg_name: "pve".to_string(),
            segtype: Some("thin".to_string()),
        };

        let result = provider.accept_lv(&lv);
        assert!(result.is_ok());
    }

    #[test]
    fn discover_finds_volumes() {
        let lvs = vec![LvInfo {
            lv_name: "vm-123.raw".to_string(),
            vg_name: "pve".to_string(),
            segtype: Some("thin".to_string()),
        }];

        let cfg = test_config();
        let lvm = Arc::new(MockLvm { lvs });
        let block = Arc::new(MockBlock);
        let pvesh = Arc::new(MockPveSh);
        let provider = LvmThinProvider::new(&cfg, lvm, block, pvesh);

        let result = provider.discover().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].storage, "local-lvm");
        assert_eq!(result[0].disk, "vm-123.raw");
        assert_eq!(result[0].archive, "lvmthin_vm-123_raw_abcd1234.img");
    }

    #[test]
    fn cleanup_adds_snaps() {
        let runner = Arc::new(ProcessRunner::new());
        let lvm = Arc::new(crate::tooling::LvmCli::new(runner));
        let mut cleanup = Cleanup::new(lvm);

        cleanup.add("pve/snap1".to_string());
        cleanup.add("pve/snap2".to_string());
        assert_eq!(cleanup.snaps.len(), 2);
    }
}
