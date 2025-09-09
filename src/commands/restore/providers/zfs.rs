use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow, bail};

use crate::{
    commands::restore::{matcher::RestoreMatcher, providers::Provider},
    tooling::{
        FsPort, PveshPort, ZfsPort,
        pbs::{PbsFile, PbsSnapshot},
        pvesh::Storage,
    },
    utils::naming::parse_archive_name,
    volume::Volume,
};

pub struct ZfsRestore<'a> {
    dest_root: String,
    target_name: String,
    snapshot: Option<&'a PbsSnapshot>,
    zfs: Arc<dyn ZfsPort>,
    pvesh: Arc<dyn PveshPort>,
    fs: Arc<dyn FsPort>,
    matcher: Arc<RestoreMatcher>,
}

impl<'a> ZfsRestore<'a> {
    pub fn new(
        snapshot: Option<&'a PbsSnapshot>,
        zfs: Arc<dyn ZfsPort>,
        pvesh: Arc<dyn PveshPort>,
        fs: Arc<dyn FsPort>,
        matcher: Arc<RestoreMatcher>,
        dest_root: String,
        target_name: String,
    ) -> Self {
        assert!(!dest_root.trim().is_empty(), "[zfs target] empty root");
        assert!(
            !target_name.trim().is_empty(),
            "[zfs target] empty target_name"
        );

        Self {
            dest_root,
            target_name,
            snapshot,
            zfs,
            pvesh,
            fs,
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

    fn resolve_dataset_target(&self, archive: &str) -> Result<(PathBuf, String)> {
        let (_provider, leaf, _id) = parse_archive_name(archive)?;

        let (size_bytes, file_name_for_err) = {
            let snap = self
                .snapshot
                .ok_or_else(|| anyhow!("no snapshot context to size '{archive}'"))?;
            let file = snap
                .files
                .iter()
                .find(|f| f.filename == archive)
                .ok_or_else(|| anyhow!("archive {archive} not found in snapshot"))?;

            (file.size, file.filename.clone())
        };
        let dataset = format!("{}/{}", self.dest_root, leaf);

        let mp = match self.zfs.dataset_mountpoint(&dataset) {
            Ok(mp) => mp,
            Err(_) => {
                self.zfs
                    .create_zvol(&dataset, size_bytes)
                    .with_context(|| format!("zfs create -V {size_bytes} {dataset}"))?;
                None
            }
        };

        let target = match mp {
            None => Path::new("/dev/zvol").join(&dataset),
            Some(path) => {
                let target = Path::new(&path).join(&leaf);
                self.fs
                    .ensure_parent_dir(&target)
                    .with_context(|| format!("create dir for {}", target.display()))?;
                self.fs
                    .create_sparse_file(&target, size_bytes)
                    .with_context(|| {
                        format!(
                            "create sparse file {} ({} bytes) for {}",
                            target.display(),
                            size_bytes,
                            file_name_for_err
                        )
                    })?;
                target
            }
        };

        Ok((target, leaf))
    }
}

impl<'a> Provider for ZfsRestore<'a> {
    fn name(&self) -> &'static str {
        "zfs"
    }

    fn collect_restore(&mut self, archive: Option<&str>, all: bool) -> Result<Vec<Volume>> {
        let mut out = Vec::new();
        let storages = self.pvesh.get_storage()?;
        let storage_id = find_storage(&storages, &self.dest_root)?;

        match (archive, all, self.snapshot) {
            (Some(a), _, Some(_snap)) => {
                if let Some(file) = _snap.files.iter().find(|f| f.filename == a)
                    && self.routes_to_me(file)
                {
                    let (target, leaf) = self.resolve_dataset_target(a)?;
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
                        let (target, leaf) = self.resolve_dataset_target(&f.filename)?;
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
fn find_storage<'a>(storages: &'a [Storage], pool: &str) -> Result<&'a str> {
    storages
        .iter()
        .find_map(|s| match *s {
            Storage::ZfsPool {
                ref id,
                pool: ref zpool_name,
                ..
            } if zpool_name.as_str() == pool => Some(id.as_str()),
            _ => None,
        })
        .ok_or_else(|| anyhow!("Zfs storage with pool='{pool}' not found"))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use anyhow::{Ok, Result};

    use super::*;
    use crate::{
        commands::restore::matcher::RestoreMatcher,
        config::{Backup, Config, Pbs, Restore, RestoreTarget},
        tooling::{FsPort, PveshPort, ZfsPort, pbs::PbsFile, pvesh::Storage},
    };

    struct MockPvesh;
    impl PveshPort for MockPvesh {
        fn get_storage(&self) -> Result<Vec<Storage>> {
            Ok(vec![Storage::ZfsPool {
                id: "local-zfs".to_string(),
                pool: "tank".to_string(),
                content: vec!["".to_string()],
            }])
        }
    }

    struct MockZfs {
        exists: bool,
        mountpoint: Option<String>,
    }

    impl ZfsPort for MockZfs {
        fn list_volumes(&self, _pool: &str) -> Result<Vec<crate::tooling::zfs::ZfsVolume>> {
            Ok(vec![])
        }
        fn guid_map(&self, _pool: &str) -> Result<std::collections::HashMap<String, String>> {
            Ok(std::collections::HashMap::new())
        }
        fn snapshot(&self, _name: &str) -> Result<()> {
            Ok(())
        }
        fn clone_readonly_dev(&self, _snap: &str, _clone: &str) -> Result<()> {
            Ok(())
        }
        fn destroy_recursive(&self, _name: &str) -> Result<()> {
            Ok(())
        }
        fn assert_dataset_exists(&self, _dataset: &str) -> Result<()> {
            if self.exists {
                Ok(())
            } else {
                bail!("dataset not found")
            }
        }
        fn dataset_mountpoint(&self, _dataset: &str) -> Result<Option<String>> {
            Ok(self.mountpoint.clone())
        }
        fn create_zvol(&self, _dataset: &str, _size_bytes: u64) -> Result<()> {
            Ok(())
        }
    }

    struct MockFs;

    impl FsPort for MockFs {
        fn ensure_dir(&self, _dir: &std::path::Path) -> Result<()> {
            Ok(())
        }
        fn ensure_parent_dir(&self, _path: &std::path::Path) -> Result<()> {
            Ok(())
        }
        fn create_sparse_file(&self, _path: &std::path::Path, _size_bytes: u64) -> Result<()> {
            Ok(())
        }
    }

    fn test_config() -> Config {
        let mut targets = BTreeMap::new();
        targets.insert(
            "zfs-tank".to_string(),
            RestoreTarget::Zfs {
                root: "tank".to_string(),
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
                    match_provider: "zfs".to_string(),
                    match_archive_regex: None,
                    target: "zfs-tank".to_string(),
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
                    filename: "zfs_vm-123_raw_abcd1234.img".to_string(),
                    size: 4 * 1024 * 1024,
                },
                PbsFile {
                    filename: "lvmthin_vm-456_raw_efgh5678.img".to_string(),
                    size: 4 * 1024 * 1024,
                },
            ],
        }
    }

    #[test]
    fn resolve_dataset_target_zvol() {
        let snap = test_snapshot();
        let zfs = Arc::new(MockZfs {
            exists: true,
            mountpoint: None,
        });
        let pvesh = Arc::new(MockPvesh);
        let fs = Arc::new(MockFs);
        let cfg = test_config();
        let matcher = Arc::new(RestoreMatcher::new(&cfg).unwrap());
        let restore = ZfsRestore::new(
            Some(&snap),
            zfs,
            pvesh,
            fs,
            matcher,
            "tank".to_string(),
            "zfs-tank".to_string(),
        );

        let (target, _) = restore
            .resolve_dataset_target("zfs_vm-123_raw_abcd1234.img")
            .unwrap();
        assert_eq!(target, PathBuf::from("/dev/zvol/tank/vm-123.raw"));
    }

    #[test]
    fn resolve_dataset_target_mounted() {
        let snap = test_snapshot();
        let zfs = Arc::new(MockZfs {
            exists: true,
            mountpoint: Some("/mnt/tank".to_string()),
        });
        let pvesh = Arc::new(MockPvesh);
        let fs = Arc::new(MockFs);
        let cfg = test_config();
        let matcher = Arc::new(RestoreMatcher::new(&cfg).unwrap());
        let restore = ZfsRestore::new(
            Some(&snap),
            zfs,
            pvesh,
            fs,
            matcher,
            "tank".to_string(),
            "zfs-tank".to_string(),
        );

        let (target, _) = restore
            .resolve_dataset_target("zfs_vm-123_raw_abcd1234.img")
            .unwrap();
        assert_eq!(target, PathBuf::from("/mnt/tank/vm-123.raw"));
    }

    #[test]
    fn collect_restore_single_archive() {
        let snap = test_snapshot();
        let zfs = Arc::new(MockZfs {
            exists: true,
            mountpoint: None,
        });
        let pvesh = Arc::new(MockPvesh);
        let fs = Arc::new(MockFs);
        let cfg = test_config();
        let matcher = Arc::new(RestoreMatcher::new(&cfg).unwrap());
        let mut restore = ZfsRestore::new(
            Some(&snap),
            zfs,
            pvesh,
            fs,
            matcher,
            "tank".to_string(),
            "zfs-tank".to_string(),
        );

        let items = restore
            .collect_restore(Some("zfs_vm-123_raw_abcd1234.img"), false)
            .unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].archive, "zfs_vm-123_raw_abcd1234.img");
        assert_eq!(items[0].device, PathBuf::from("/dev/zvol/tank/vm-123.raw"));
    }

    #[test]
    fn collect_restore_all_archives() {
        let snap = test_snapshot();
        let zfs = Arc::new(MockZfs {
            exists: true,
            mountpoint: None,
        });
        let pvesh = Arc::new(MockPvesh);
        let fs = Arc::new(MockFs);
        let cfg = test_config();
        let matcher = Arc::new(RestoreMatcher::new(&cfg).unwrap());
        let mut restore = ZfsRestore::new(
            Some(&snap),
            zfs,
            pvesh,
            fs,
            matcher,
            "tank".to_string(),
            "zfs-tank".to_string(),
        );

        let items = restore.collect_restore(None, true).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].archive, "zfs_vm-123_raw_abcd1234.img");
    }

    #[test]
    fn list_archives_filters_zfs() {
        let snap = test_snapshot();
        let zfs = Arc::new(MockZfs {
            exists: true,
            mountpoint: None,
        });
        let pvesh = Arc::new(MockPvesh);
        let fs = Arc::new(MockFs);
        let cfg = test_config();
        let matcher = Arc::new(RestoreMatcher::new(&cfg).unwrap());
        let restore = ZfsRestore::new(
            Some(&snap),
            zfs,
            pvesh,
            fs,
            matcher,
            "tank".to_string(),
            "zfs-tank".to_string(),
        );

        let archives = restore.list_archives(&snap);
        assert_eq!(archives.len(), 1);
        assert_eq!(archives[0], "zfs_vm-123_raw_abcd1234.img");
    }

    #[test]
    fn resolve_dataset_target_missing_dataset_errors() {
        let zfs = Arc::new(MockZfs {
            exists: false,
            mountpoint: None,
        });
        let pvesh = Arc::new(MockPvesh);
        let fs = Arc::new(MockFs);
        let cfg = test_config();
        let matcher = Arc::new(RestoreMatcher::new(&cfg).unwrap());
        let restore = ZfsRestore::new(
            None,
            zfs,
            pvesh,
            fs,
            matcher,
            "tank".to_string(),
            "zfs-tank".to_string(),
        );
        assert!(
            restore
                .resolve_dataset_target("zfs_vm-123_raw_abcd1234.img")
                .is_err()
        );
    }

    #[test]
    fn collect_restore_all_requires_snapshot() {
        let zfs = Arc::new(MockZfs {
            exists: true,
            mountpoint: None,
        });
        let pvesh = Arc::new(MockPvesh);
        let fs = Arc::new(MockFs);
        let cfg = test_config();
        let matcher = Arc::new(RestoreMatcher::new(&cfg).unwrap());
        let mut restore = ZfsRestore::new(
            None,
            zfs,
            pvesh,
            fs,
            matcher,
            "tank".to_string(),
            "zfs-tank".to_string(),
        );
        assert!(restore.collect_restore(None, true).is_err());
    }
}
