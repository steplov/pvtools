use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow, bail};

use crate::{
    commands::restore::providers::Provider,
    config::Config,
    tooling::{FsPort, PbsSnapshot, PveshPort, ZfsPort, pvesh::Storage},
    utils::naming::parse_archive_name,
    volume::Volume,
};

pub struct ZfsRestore<'a> {
    dest_root: String,
    snapshot: Option<&'a PbsSnapshot>,
    zfs: Arc<dyn ZfsPort>,
    pvesh: Arc<dyn PveshPort>,
    fs: Arc<dyn FsPort>,
}

impl<'a> ZfsRestore<'a> {
    pub fn new(
        cfg: &Config,
        snapshot: Option<&'a PbsSnapshot>,
        zfs: Arc<dyn ZfsPort>,
        pvesh: Arc<dyn PveshPort>,
        fs: Arc<dyn FsPort>,
    ) -> Self {
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
            zfs,
            pvesh,
            fs,
        }
    }

    fn resolve_dataset_target(&self, archive: &str) -> Result<(PathBuf, String)> {
        let (provider, leaf, _id) = parse_archive_name(archive)?;

        if provider != "zfs" {
            bail!("not a zfs archive: {archive}");
        }

        let dataset = format!("{}/{}", self.dest_root, leaf);

        let (size_bytes, file_name_for_err) = {
            let snap = self
                .snapshot
                .ok_or_else(|| anyhow!("no snapshot context to size '{archive}'"))?;
            let file = snap
                .files
                .iter()
                .find(|f| f.filename == archive)
                .ok_or_else(|| anyhow!("archive {archive} not found in snapshot"))?;
            let sz = file.size;
            (sz, file.filename.clone())
        };

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
        "zfs-restore"
    }

    fn collect_restore(&mut self, archive: Option<&str>, all: bool) -> Result<Vec<Volume>> {
        let mut out = Vec::new();
        let storages = self.pvesh.get_storage()?;
        let storage_id = find_storage(&storages, &self.dest_root)?;

        match (archive, all, self.snapshot) {
            (Some(a), _, Some(_snap)) => {
                if a.starts_with("zfs_") {
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
                    if f.filename.starts_with("zfs_") {
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
            (None, false, _) => { /* ничего не выбирали */ }
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
    use std::{collections::HashMap, sync::Arc};

    use anyhow::{Ok, Result};

    use super::*;
    use crate::{
        config::{Config, Pbs, Zfs, ZfsRestore as ZfsRestoreConfig},
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
        fn guid_map(&self, _pool: &str) -> Result<HashMap<String, String>> {
            Ok(HashMap::new())
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
            zfs: Some(Zfs {
                pools: vec!["tank".to_string()],
                restore: Some(ZfsRestoreConfig {
                    dest_root: "tank".to_string(),
                }),
            }),
            lvmthin: None,
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
        let cfg = test_config();
        let snap = test_snapshot();
        let zfs = Arc::new(MockZfs {
            exists: true,
            mountpoint: None,
        });
        let pvesh = Arc::new(MockPvesh);
        let fs = Arc::new(MockFs);
        let restore = ZfsRestore::new(&cfg, Some(&snap), zfs, pvesh, fs);

        let (target, _) = restore
            .resolve_dataset_target("zfs_vm-123_raw_abcd1234.img")
            .unwrap();
        assert_eq!(target, PathBuf::from("/dev/zvol/tank/vm-123.raw"));
    }

    #[test]
    fn resolve_dataset_target_mounted() {
        let cfg = test_config();
        let snap = test_snapshot();
        let zfs = Arc::new(MockZfs {
            exists: true,
            mountpoint: Some("/mnt/tank".to_string()),
        });
        let pvesh = Arc::new(MockPvesh);
        let fs = Arc::new(MockFs);
        let restore = ZfsRestore::new(&cfg, Some(&snap), zfs, pvesh, fs);

        let (target, _) = restore
            .resolve_dataset_target("zfs_vm-123_raw_abcd1234.img")
            .unwrap();
        assert_eq!(target, PathBuf::from("/mnt/tank/vm-123.raw"));
    }

    #[test]
    fn resolve_dataset_target_rejects_non_zfs() {
        let cfg = test_config();
        let zfs = Arc::new(MockZfs {
            exists: true,
            mountpoint: None,
        });
        let pvesh = Arc::new(MockPvesh);
        let fs = Arc::new(MockFs);
        let restore = ZfsRestore::new(&cfg, None, zfs, pvesh, fs);

        let result = restore.resolve_dataset_target("lvmthin_vm-123_raw_abcd1234.img");
        assert!(result.is_err());
    }

    #[test]
    fn collect_restore_single_archive() {
        let cfg = test_config();
        let snap = test_snapshot();
        let zfs = Arc::new(MockZfs {
            exists: true,
            mountpoint: None,
        });
        let pvesh = Arc::new(MockPvesh);
        let fs = Arc::new(MockFs);
        let mut restore = ZfsRestore::new(&cfg, Some(&snap), zfs, pvesh, fs);

        let items = restore
            .collect_restore(Some("zfs_vm-123_raw_abcd1234.img"), false)
            .unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].archive, "zfs_vm-123_raw_abcd1234.img");
        assert_eq!(items[0].device, PathBuf::from("/dev/zvol/tank/vm-123.raw"));
    }

    #[test]
    fn collect_restore_all_archives() {
        let cfg = test_config();
        let snap = test_snapshot();
        let zfs = Arc::new(MockZfs {
            exists: true,
            mountpoint: None,
        });
        let pvesh = Arc::new(MockPvesh);
        let fs = Arc::new(MockFs);
        let mut restore = ZfsRestore::new(&cfg, Some(&snap), zfs, pvesh, fs);

        let items = restore.collect_restore(None, true).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].archive, "zfs_vm-123_raw_abcd1234.img");
    }

    #[test]
    fn list_archives_filters_zfs() {
        let cfg = test_config();
        let snap = test_snapshot();
        let zfs = Arc::new(MockZfs {
            exists: true,
            mountpoint: None,
        });
        let pvesh = Arc::new(MockPvesh);
        let fs = Arc::new(MockFs);
        let restore = ZfsRestore::new(&cfg, Some(&snap), zfs, pvesh, fs);

        let archives = restore.list_archives(&snap);
        assert_eq!(archives.len(), 1);
        assert_eq!(archives[0], "zfs_vm-123_raw_abcd1234.img");
    }

    #[test]
    fn resolve_dataset_target_missing_dataset_errors() {
        let cfg = test_config();
        let zfs = Arc::new(MockZfs {
            exists: false,
            mountpoint: None,
        });
        let pvesh = Arc::new(MockPvesh);
        let fs = Arc::new(MockFs);
        let restore = ZfsRestore::new(&cfg, None, zfs, pvesh, fs);
        assert!(
            restore
                .resolve_dataset_target("zfs_vm-123_raw_abcd1234.img")
                .is_err()
        );
    }

    #[test]
    fn collect_restore_all_requires_snapshot() {
        let cfg = test_config();
        let zfs = Arc::new(MockZfs {
            exists: true,
            mountpoint: None,
        });
        let pvesh = Arc::new(MockPvesh);
        let fs = Arc::new(MockFs);
        let mut restore = ZfsRestore::new(&cfg, None, zfs, pvesh, fs);
        assert!(restore.collect_restore(None, true).is_err());
    }
}
