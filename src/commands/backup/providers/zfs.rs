use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result, anyhow};
use tracing as log;

use crate::{
    commands::backup::providers::Provider,
    config::{Config, Pbs},
    tooling::{BlockPort, PveshPort, ZfsPort, pvesh::Storage},
    utils::{exec_policy, naming::create_archive_name, path::dataset_leaf, time::current_epoch},
    volume::Volume,
};

const DEV_PREFIX: &str = "/dev/zvol/";
const CLONE_SUFFIX: &str = "pvtools";

enum Reject<'a> {
    NotBase(&'a str),
    PvDenied(&'a str),
}

#[derive(Debug, Clone)]
struct ZfsMeta {
    dataset: String,
    run_ts: u64,
}

#[derive(Debug, Clone)]
struct ZfsNames {
    snap: String,
    clone: String,
    device: PathBuf,
}

pub struct ZfsProvider<'a> {
    pools: &'a [String],
    pbs: &'a Pbs,
    run_ts: u64,
    cleanup: Cleanup,
    zfs: Arc<dyn ZfsPort>,
    block: Arc<dyn BlockPort>,
    pvesh: Arc<dyn PveshPort>,
}

impl<'a> ZfsProvider<'a> {
    pub fn new(
        cfg: &'a Config,
        zfs: Arc<dyn ZfsPort>,
        block: Arc<dyn BlockPort>,
        pvesh: Arc<dyn PveshPort>,
    ) -> Self {
        let z = cfg.zfs.as_ref().expect("[zfs] missing");

        Self {
            pools: &z.pools,
            pbs: &cfg.pbs,
            run_ts: current_epoch(),
            cleanup: Cleanup::new(zfs.clone()),
            zfs,
            block,
            pvesh,
        }
    }

    #[inline]
    fn accept_ds<'b>(
        &self,
        name: &'b str,
        origin: Option<&'b str>,
    ) -> std::result::Result<(), Reject<'b>> {
        if let Some(orig) = origin {
            return Err(Reject::NotBase(orig));
        }
        let leaf = dataset_leaf(name);
        if !self.pbs.pv_allows(leaf) {
            return Err(Reject::PvDenied(leaf));
        }
        Ok(())
    }
}

impl<'a> Provider for ZfsProvider<'a> {
    fn name(&self) -> &'static str {
        "zfs"
    }

    fn discover(&self) -> Result<Vec<Volume>> {
        let mut out = Vec::<Volume>::new();
        let storages = self.pvesh.get_storage()?;

        for pool in self.pools {
            let zfs_volumes = self.zfs.list_volumes(pool)?;
            let guid_map = self.zfs.guid_map(pool)?;
            let storage_id = find_storage(&storages, pool)?;

            for v in zfs_volumes {
                let name = &v.name;
                let origin = v.origin.as_deref();
                match self.accept_ds(name, origin) {
                    Ok(()) => {
                        let leaf = dataset_leaf(name);
                        let id8 = guid_map.get(name).ok_or_else(|| {
                            anyhow::anyhow!("guid not found for dataset {}", name)
                        })?;
                        let archive = create_archive_name("zfs", leaf, id8)?;

                        let names = build_zfs_names(name, CLONE_SUFFIX, self.run_ts);
                        let device = names.device.clone();

                        out.push(Volume {
                            storage: storage_id.to_string(),
                            disk: leaf.to_string(),
                            archive,
                            device,
                            meta: Some(Arc::new(ZfsMeta {
                                dataset: name.to_string(),
                                run_ts: self.run_ts,
                            })),
                        });
                    }
                    Err(Reject::NotBase(orig)) => {
                        log::trace!("skip {}: origin != '-' (origin='{}')", &name, orig)
                    }
                    Err(Reject::PvDenied(leaf)) => {
                        log::trace!("skip {}: pv_allows(false) for leaf '{}'", &name, leaf)
                    }
                }
            }
        }

        if out.is_empty() {
            log::debug!("zfs: no candidate volumes");
        }

        Ok(out)
    }

    fn prepare(&mut self, volumes: &[Volume]) -> Result<()> {
        for v in volumes {
            let meta = match v.meta::<ZfsMeta>() {
                Some(m) => m,
                None => continue,
            };

            let names = build_zfs_names(&meta.dataset, CLONE_SUFFIX, meta.run_ts);

            self.zfs
                .snapshot(&names.snap)
                .with_context(|| format!("zfs snapshot on {}", &meta.dataset))?;
            self.zfs
                .clone_readonly_dev(&names.snap, &names.clone)
                .with_context(|| format!("zfs clone on {}", &meta.dataset))?;

            if !exec_policy::is_dry_run() {
                self.block.wait_for_block(&names.device)?;
                self.cleanup
                    .add_many([names.clone.clone(), names.snap.clone()]);
            }
        }

        Ok(())
    }
}

#[derive(Default)]
struct Cleanup {
    tasks: Vec<String>,
    zfs: Option<Arc<dyn ZfsPort>>,
}

impl Cleanup {
    pub fn new(zfs: Arc<dyn ZfsPort>) -> Self {
        Self {
            tasks: Vec::new(),
            zfs: Some(zfs),
        }
    }

    fn add_many<I: IntoIterator<Item = String>>(&mut self, snaps: I) {
        for s in snaps {
            self.tasks.push(s);
        }
    }
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        if let Some(zfs) = &self.zfs {
            for s in self.tasks.drain(..) {
                if let Err(e) = zfs.destroy_recursive(&s) {
                    log::warn!("[cleanup] zfs destroy -r {} failed: {e}", s);
                }
            }
        }
    }
}

#[inline]
fn build_zfs_names(ds: &str, suffix: &str, ts: u64) -> ZfsNames {
    let snap = format!("{ds}@{suffix}-{ts}");
    let clone = format!("{ds}-{suffix}-{ts}");
    let device = PathBuf::from(format!("{DEV_PREFIX}{clone}"));
    ZfsNames {
        snap,
        clone,
        device,
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
    use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

    use anyhow::Result;

    use super::*;
    use crate::{
        config::{Config, Pbs, Zfs},
        tooling::{BlockPort, ZfsPort, zfs::ZfsVolume},
        utils::process::ProcessRunner,
    };

    struct MockZfs {
        volumes: Vec<ZfsVolume>,
        guid_map: HashMap<String, String>,
    }

    impl ZfsPort for MockZfs {
        fn list_volumes(&self, _pool: &str) -> Result<Vec<ZfsVolume>> {
            Ok(self.volumes.clone())
        }
        fn guid_map(&self, _pool: &str) -> Result<HashMap<String, String>> {
            Ok(self.guid_map.clone())
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
            Ok(())
        }
        fn dataset_mountpoint(&self, _dataset: &str) -> Result<Option<String>> {
            Ok(None)
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
        fn get_storage(&self) -> Result<Vec<crate::tooling::pvesh::Storage>> {
            Ok(vec![crate::tooling::pvesh::Storage::ZfsPool {
                id: "local-zfs".to_string(),
                pool: "tank".to_string(),
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
            zfs: Some(Zfs {
                pools: vec!["tank".to_string()],
                restore: None,
            }),
            lvmthin: None,
        }
    }

    #[test]
    fn build_zfs_names_correct() {
        let names = build_zfs_names("tank/vm-123", "pvtools", 1234567890);
        assert_eq!(names.snap, "tank/vm-123@pvtools-1234567890");
        assert_eq!(names.clone, "tank/vm-123-pvtools-1234567890");
        assert_eq!(
            names.device,
            PathBuf::from("/dev/zvol/tank/vm-123-pvtools-1234567890")
        );
    }

    #[test]
    fn accept_ds_rejects_clone() {
        let cfg = test_config();
        let zfs = Arc::new(MockZfs {
            volumes: vec![],
            guid_map: HashMap::new(),
        });
        let block = Arc::new(MockBlock);
        let pvesh = Arc::new(MockPveSh);
        let provider = ZfsProvider::new(&cfg, zfs, block, pvesh);

        let result = provider.accept_ds("tank/vm-123", Some("tank/vm-base@snap"));
        assert!(matches!(result, Err(Reject::NotBase(_))));
    }

    #[test]
    fn accept_ds_rejects_non_pv() {
        let cfg = test_config();
        let zfs = Arc::new(MockZfs {
            volumes: vec![],
            guid_map: HashMap::new(),
        });
        let block = Arc::new(MockBlock);
        let pvesh = Arc::new(MockPveSh);
        let provider = ZfsProvider::new(&cfg, zfs, block, pvesh);

        let result = provider.accept_ds("tank/other-123", None);
        assert!(matches!(result, Err(Reject::PvDenied(_))));
    }

    #[test]
    fn accept_ds_allows_valid() {
        let cfg = test_config();
        let zfs = Arc::new(MockZfs {
            volumes: vec![],
            guid_map: HashMap::new(),
        });
        let block = Arc::new(MockBlock);
        let pvesh = Arc::new(MockPveSh);
        let provider = ZfsProvider::new(&cfg, zfs, block, pvesh);

        let result = provider.accept_ds("tank/vm-123", None);
        assert!(result.is_ok());
    }

    #[test]
    fn discover_finds_volumes() {
        let mut guid_map = HashMap::new();
        guid_map.insert("tank/vm-123.raw".to_string(), "abcd1234".to_string());

        let volumes = vec![ZfsVolume {
            name: "tank/vm-123.raw".to_string(),
            origin: None,
        }];

        let cfg = test_config();
        let zfs = Arc::new(MockZfs { volumes, guid_map });
        let block = Arc::new(MockBlock);
        let pvesh = Arc::new(MockPveSh);
        let provider = ZfsProvider::new(&cfg, zfs, block, pvesh);

        let result = provider.discover().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].storage, "local-zfs");
        assert_eq!(result[0].disk, "vm-123.raw");
        assert_eq!(result[0].archive, "zfs_vm-123_raw_abcd1234.img");
    }

    #[test]
    fn cleanup_adds_tasks() {
        let runner = Arc::new(ProcessRunner::new());
        let zfs = Arc::new(crate::tooling::ZfsCli::new(runner));
        let mut cleanup = Cleanup::new(zfs);

        cleanup.add_many(vec!["snap1".to_string(), "snap2".to_string()]);
        assert_eq!(cleanup.tasks.len(), 2);
    }
}
