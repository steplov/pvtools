use std::{path::PathBuf, sync::Arc};

use anyhow::{Result, anyhow, bail};

use crate::{
    commands::restore::{matcher::RestoreMatcher, providers::Provider},
    tooling::{
        LvmPort, PveshPort,
        pbs::{PbsFile, PbsSnapshot},
        pvesh::Storage,
    },
    utils::naming::parse_archive_name,
    volume::Volume,
};

pub struct LvmthinRestore<'a> {
    vg: String,
    thinpool: String,
    target_name: String,
    snapshot: Option<&'a PbsSnapshot>,
    lvm: Arc<dyn LvmPort>,
    pvesh: Arc<dyn PveshPort>,
    matcher: Arc<RestoreMatcher>,
}

impl<'a> LvmthinRestore<'a> {
    pub fn new(
        snapshot: Option<&'a PbsSnapshot>,
        lvm: Arc<dyn LvmPort>,
        pvesh: Arc<dyn PveshPort>,
        matcher: Arc<RestoreMatcher>,
        vg: String,
        thinpool: String,
        target_name: String,
    ) -> Self {
        assert!(!vg.trim().is_empty(), "[lvmthin target] empty vg");
        assert!(
            !thinpool.trim().is_empty(),
            "[lvmthin target] empty thinpool"
        );
        assert!(
            !target_name.trim().is_empty(),
            "[lvmthin target] empty target_name"
        );
        Self {
            vg,
            thinpool,
            target_name,
            snapshot,
            lvm,
            pvesh,
            matcher,
        }
    }

    #[inline]
    fn routes_to_me(&self, f: &PbsFile) -> bool {
        if let Ok((provider, _leaf, _id)) = parse_archive_name(&f.filename)
            && let Some(tname) = self.matcher.pick_target_name(&provider, f)
        {
            return tname == self.target_name;
        }
        false
    }

    fn resolve_lv_target(&self, archive: &str) -> Result<(PathBuf, String)> {
        let (_provider, leaf, _id) = parse_archive_name(archive)?;

        let exists = self.lvm.lv_name(&self.vg, &leaf).is_ok();

        if !exists {
            let snap = self
                .snapshot
                .ok_or_else(|| anyhow!("no snapshot context to size '{archive}'"))?;
            let file = snap
                .files
                .iter()
                .find(|f| f.filename == archive)
                .ok_or_else(|| anyhow!("archive {archive} not found in snapshot"))?;
            let size_bytes = file.size;

            self.lvm
                .lvcreate_thin(&self.vg, &self.thinpool, &leaf, size_bytes)?;
            let lv_fq = format!("{}/{}", self.vg, leaf);
            self.lvm.lvchange_activate(&lv_fq)?;
        }

        let lv_path = format!("/dev/{}/{}", self.vg, leaf);

        Ok((PathBuf::from(lv_path), leaf))
    }
}

impl<'a> Provider for LvmthinRestore<'a> {
    fn name(&self) -> &'static str {
        "lvmthin"
    }

    fn collect_restore(&mut self, archive: Option<&str>, all: bool) -> Result<Vec<Volume>> {
        let mut out = Vec::new();
        let storages = self.pvesh.get_storage()?;
        let storage_id = find_storage(&storages, &self.vg)?;
        match (archive, all, self.snapshot) {
            (Some(a), _, Some(snap)) => {
                if let Some(file) = snap.files.iter().find(|f| f.filename == a)
                    && self.routes_to_me(file)
                {
                    let (target, leaf) = self.resolve_lv_target(a)?;
                    out.push(Volume {
                        storage: storage_id.to_string(),
                        disk: leaf,
                        archive: a.to_string(),
                        device: target,
                        meta: None,
                    });
                }
            }
            (None, true, Some(snap)) => {
                for f in &snap.files {
                    if self.routes_to_me(f) {
                        let (target, leaf) = self.resolve_lv_target(&f.filename)?;
                        out.push(Volume {
                            storage: storage_id.to_string(),
                            disk: leaf,
                            archive: f.filename.clone(),
                            device: target,
                            meta: None,
                        });
                    }
                }
            }
            (Some(a), _, None) => bail!("no snapshot context for archive {a}"),
            (None, true, None) => bail!("no snapshot context provided for restore-all"),
            (None, false, _) => {}
        }

        Ok(out)
    }

    fn list_archives(&self, snap: &PbsSnapshot) -> Vec<String> {
        snap.files
            .iter()
            .filter(|f| self.routes_to_me(f))
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
    use std::{collections::BTreeMap, sync::Arc};

    use anyhow::Result;

    use super::*;
    use crate::{
        commands::restore::matcher::RestoreMatcher,
        config::{Backup, Config, Pbs, Restore, RestoreTarget},
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
        fn lv_name(&self, _vg: &str, _leaf: &str) -> Result<String> {
            bail!("LV not found")
        }
        fn lv_uuid_short8(&self, _vg: &str, _lv: &str) -> Result<String> {
            Ok("abcd1234".to_string())
        }
        fn lvcreate_thin(
            &self,
            _vg: &str,
            _thinpool: &str,
            _name: &str,
            _size_bytes: u64,
        ) -> Result<()> {
            Ok(())
        }
    }

    fn test_config() -> Config {
        let mut targets = BTreeMap::new();
        targets.insert(
            "lvm-pve".to_string(),
            RestoreTarget::LvmThin {
                vg: "pve".to_string(),
                thinpool: Some("data".to_string()),
            },
        );

        Config {
            pbs: Pbs {
                repos: std::collections::HashMap::new(),
                keyfile: None,
                password: None,
                ns: None,
                backup_id: "test".to_string(),
            },
            backup: Backup::default(),
            restore: Restore {
                targets,
                rules: vec![crate::config::RestoreRule {
                    match_provider: "lvmthin".to_string(),
                    match_archive_regex: None,
                    target: "lvm-pve".to_string(),
                }],
                default_target: None,
            },
        }
    }

    fn test_snapshot() -> PbsSnapshot {
        PbsSnapshot {
            backup_id: "test".to_string(),
            backup_time: 1234567890,
            files: vec![
                PbsFile {
                    filename: "lvmthin_vm-123_raw_abcd1234.img".to_string(),
                    size: 4 * 1024 * 1024,
                },
                PbsFile {
                    filename: "zfs_vm-456_raw_efgh5678.img".to_string(),
                    size: 4 * 1024 * 1024,
                },
            ],
        }
    }

    #[test]
    fn resolve_lv_target_correct() {
        let snap = test_snapshot();
        let lvm = Arc::new(MockLvm);
        let pvesh = Arc::new(MockPvesh);
        let cfg = test_config();
        let matcher = Arc::new(RestoreMatcher::new(&cfg).unwrap());
        let restore = LvmthinRestore::new(
            Some(&snap),
            lvm,
            pvesh,
            matcher,
            "pve".to_string(),
            "data".to_string(),
            "lvm-pve".to_string(),
        );

        let (target, _) = restore
            .resolve_lv_target("lvmthin_vm-123_raw_abcd1234.img")
            .unwrap();
        assert_eq!(target, PathBuf::from("/dev/pve/vm-123.raw"));
    }

    #[test]
    fn collect_restore_single_archive() {
        let snap = test_snapshot();
        let lvm = Arc::new(MockLvm);
        let pvesh = Arc::new(MockPvesh);
        let cfg = test_config();
        let matcher = Arc::new(RestoreMatcher::new(&cfg).unwrap());
        let mut restore = LvmthinRestore::new(
            Some(&snap),
            lvm,
            pvesh,
            matcher,
            "pve".to_string(),
            "data".to_string(),
            "lvm-pve".to_string(),
        );

        let items = restore
            .collect_restore(Some("lvmthin_vm-123_raw_abcd1234.img"), false)
            .unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].archive, "lvmthin_vm-123_raw_abcd1234.img");
        assert_eq!(items[0].device, PathBuf::from("/dev/pve/vm-123.raw"));
    }

    #[test]
    fn collect_restore_all_archives() {
        let snap = test_snapshot();
        let lvm = Arc::new(MockLvm);
        let pvesh = Arc::new(MockPvesh);
        let cfg = test_config();
        let matcher = Arc::new(RestoreMatcher::new(&cfg).unwrap());
        let mut restore = LvmthinRestore::new(
            Some(&snap),
            lvm,
            pvesh,
            matcher,
            "pve".to_string(),
            "data".to_string(),
            "lvm-pve".to_string(),
        );

        let items = restore.collect_restore(None, true).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].archive, "lvmthin_vm-123_raw_abcd1234.img");
    }

    #[test]
    fn list_archives_filters_lvmthin() {
        let snap = test_snapshot();
        let lvm = Arc::new(MockLvm);
        let pvesh = Arc::new(MockPvesh);
        let cfg = test_config();
        let matcher = Arc::new(RestoreMatcher::new(&cfg).unwrap());
        let restore = LvmthinRestore::new(
            Some(&snap),
            lvm,
            pvesh,
            matcher,
            "pve".to_string(),
            "data".to_string(),
            "lvm-pve".to_string(),
        );

        let archives = restore.list_archives(&snap);
        assert_eq!(archives.len(), 1);
        assert_eq!(archives[0], "lvmthin_vm-123_raw_abcd1234.img");
    }
}
