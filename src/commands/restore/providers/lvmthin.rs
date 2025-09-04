use std::{path::PathBuf, sync::Arc};

use anyhow::{Result, anyhow, bail};

use crate::{
    commands::restore::providers::Provider,
    config::Config,
    tooling::{LvmPort, PbsSnapshot, PveshPort, pvesh::Storage},
    utils::naming::parse_archive_name,
    volume::Volume,
};

pub struct LvmthinRestore<'a> {
    vg: String,
    snapshot: Option<&'a PbsSnapshot>,
    lvm: Arc<dyn LvmPort>,
    pvesh: Arc<dyn PveshPort>,
}

impl<'a> LvmthinRestore<'a> {
    pub fn new(
        cfg: &Config,
        snapshot: Option<&'a PbsSnapshot>,
        lvm: Arc<dyn LvmPort>,
        pvesh: Arc<dyn PveshPort>,
    ) -> Self {
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
            lvm,
            pvesh,
        }
    }

    fn resolve_lv_target(&self, archive: &str) -> Result<(PathBuf, String)> {
        let (provider, leaf, _id) = parse_archive_name(archive)?;

        if provider != "lvmthin" {
            bail!("not an lvmthin archive: {archive}");
        }

        self.lvm.lv_name(&self.vg, &leaf)?;

        let lv_path = format!("/dev/{}/{}", self.vg, leaf);

        Ok((PathBuf::from(lv_path), leaf))
    }
}

impl<'a> Provider for LvmthinRestore<'a> {
    fn name(&self) -> &'static str {
        "lvmthin"
    }

    fn collect_restore(
        &mut self,
        archive: Option<&str>,
        all: bool,
        _force: bool,
    ) -> Result<Vec<Volume>> {
        let mut out = Vec::new();
        let storages = self.pvesh.get_storage()?;
        let storage_id = find_storage(&storages, &self.vg)?;

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
                let (target, leaf) = self.resolve_lv_target(&file.filename)?;

                out.push(Volume {
                    storage: storage_id.to_string(),
                    disk: leaf,
                    archive: file.filename.clone(),
                    device: target.clone(),
                    meta: None,
                });
            } else {
                bail!("no snapshot context for archive {a}");
            }
        } else if all {
            if let Some(snap) = self.snapshot {
                for f in &snap.files {
                    if f.filename.starts_with("lvmthin_") {
                        let (target, leaf) = self.resolve_lv_target(&f.filename)?;

                        out.push(Volume {
                            storage: storage_id.to_string(),
                            disk: leaf,
                            archive: f.filename.clone(),
                            device: target.clone(),
                            meta: None,
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

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use anyhow::Result;

    use super::*;
    use crate::{
        config::{Config, LvmThin, LvmThinRestore, Pbs},
        tooling::{LvmPort, PveshPort, pbs::PbsFile, pvesh::Storage},
    };

    struct MockPvesh;
    impl PveshPort for MockPvesh {
        fn get_storage(&self) -> Result<Vec<Storage>> {
            Ok(vec![Storage::LvmThin {
                id: "local-lvm".to_string(),
                vgname: "pve".to_string(),
                thinpool: "data".to_string(),
                content: vec!["".to_string()],
            }])
        }
    }

    struct MockLvm;

    impl LvmPort for MockLvm {
        fn list_lvs(&self) -> Result<Vec<crate::tooling::lvm::LvInfo>> {
            Ok(vec![])
        }
        fn lvcreate_snapshot(&self, _vg: &str, _lv: &str, _snap: &str) -> Result<String> {
            Ok("snap".to_string())
        }
        fn lvchange_activate(&self, _lv_fq: &str) -> Result<()> {
            Ok(())
        }
        fn lvremove_force(&self, _lv_fq: &str) -> Result<()> {
            Ok(())
        }
        fn lv_name(&self, _vg: &str, leaf: &str) -> Result<String> {
            Ok(leaf.to_string())
        }
        fn lv_uuid_short8(&self, _vg: &str, _lv: &str) -> Result<String> {
            Ok("abcd1234".to_string())
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
                pv_prefixes: vec![],
                pv_exclude_re: None,
                pv_exclude_re_src: None,
            },
            zfs: None,
            lvmthin: Some(LvmThin {
                vgs: vec!["pve".to_string()],
                restore: Some(LvmThinRestore {
                    vg: "pve".to_string(),
                    thinpool: Some("data".to_string()),
                }),
            }),
        }
    }

    fn test_snapshot() -> PbsSnapshot {
        PbsSnapshot {
            backup_id: "test".to_string(),
            backup_time: 1234567890,
            files: vec![
                PbsFile {
                    filename: "lvmthin_vm-123_raw_abcd1234.img".to_string(),
                },
                PbsFile {
                    filename: "zfs_vm-456_raw_efgh5678.img".to_string(),
                },
            ],
        }
    }

    #[test]
    fn resolve_lv_target_correct() {
        let cfg = test_config();
        let lvm = Arc::new(MockLvm);
        let pvesh = Arc::new(MockPvesh);
        let restore = LvmthinRestore::new(&cfg, None, lvm, pvesh);

        let (target, _) = restore
            .resolve_lv_target("lvmthin_vm-123_raw_abcd1234.img")
            .unwrap();
        assert_eq!(target, PathBuf::from("/dev/pve/vm-123.raw"));
    }

    #[test]
    fn resolve_lv_target_rejects_non_lvmthin() {
        let cfg = test_config();
        let lvm = Arc::new(MockLvm);
        let pvesh = Arc::new(MockPvesh);
        let restore = LvmthinRestore::new(&cfg, None, lvm, pvesh);

        let result = restore.resolve_lv_target("zfs_vm-123_raw_abcd1234.img");
        assert!(result.is_err());
    }

    #[test]
    fn collect_restore_single_archive() {
        let cfg = test_config();
        let snap = test_snapshot();
        let lvm = Arc::new(MockLvm);
        let pvesh = Arc::new(MockPvesh);
        let mut restore = LvmthinRestore::new(&cfg, Some(&snap), lvm, pvesh);

        let items = restore
            .collect_restore(Some("lvmthin_vm-123_raw_abcd1234.img"), false, false)
            .unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].archive, "lvmthin_vm-123_raw_abcd1234.img");
        assert_eq!(items[0].device, PathBuf::from("/dev/pve/vm-123.raw"));
    }

    #[test]
    fn collect_restore_all_archives() {
        let cfg = test_config();
        let snap = test_snapshot();
        let lvm = Arc::new(MockLvm);
        let pvesh = Arc::new(MockPvesh);
        let mut restore = LvmthinRestore::new(&cfg, Some(&snap), lvm, pvesh);

        let items = restore.collect_restore(None, true, false).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].archive, "lvmthin_vm-123_raw_abcd1234.img");
    }

    #[test]
    fn list_archives_filters_lvmthin() {
        let cfg = test_config();
        let snap = test_snapshot();
        let lvm = Arc::new(MockLvm);
        let pvesh = Arc::new(MockPvesh);
        let restore = LvmthinRestore::new(&cfg, Some(&snap), lvm, pvesh);

        let archives = restore.list_archives(&snap);
        assert_eq!(archives.len(), 1);
        assert_eq!(archives[0], "lvmthin_vm-123_raw_abcd1234.img");
    }
}
