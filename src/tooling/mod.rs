use std::{collections::BTreeSet, sync::Arc};

use anyhow::Result;

use crate::{
    config::Config,
    utils::{bins::ensure_bins, process::Runner},
};

pub mod block;
pub mod dd;
pub mod fs;
pub mod lvm;
pub mod pbs;
pub mod pvesh;
pub mod zfs;

pub use block::{BlockCli, BlockPort};
pub use dd::{DdCli, DdPort};
pub use fs::{FsCli, FsPort};
pub use lvm::{LvmCli, LvmPort};
pub use pbs::{PbsCli, PbsPort};
pub use pvesh::{PveshCli, PveshPort};
pub use zfs::{ZfsCli, ZfsPort};

pub struct Toolbox {
    pbs: Arc<dyn PbsPort>,
    zfs: Option<Arc<dyn ZfsPort>>,
    lvm: Option<Arc<dyn LvmPort>>,
    block: Arc<dyn BlockPort>,
    dd: Arc<dyn DdPort>,
    pvesh: Arc<dyn PveshPort>,
    fs: Arc<dyn FsPort>,
}

impl Toolbox {
    pub fn new(cfg: &Config, runner: Arc<dyn Runner + Send + Sync>) -> Result<Self> {
        ensure_bins_for_cfg(cfg)?;

        let pbs_cfg = Arc::new(cfg.pbs.clone());
        let pbs: Arc<dyn PbsPort> = Arc::new(PbsCli::new(runner.clone(), pbs_cfg));

        let zfs: Option<Arc<dyn ZfsPort>> = if cfg.backup.sources.zfs.is_some() {
            Some(Arc::new(ZfsCli::new(runner.clone())) as Arc<dyn ZfsPort>)
        } else {
            None
        };
        let lvm: Option<Arc<dyn LvmPort>> = if cfg.backup.sources.lvmthin.is_some() {
            Some(Arc::new(LvmCli::new(runner.clone())) as Arc<dyn LvmPort>)
        } else {
            None
        };
        let block = Arc::new(BlockCli::new(runner.clone())) as Arc<dyn BlockPort>;
        let dd = Arc::new(DdCli::new()) as Arc<dyn DdPort>;
        let pvesh = Arc::new(PveshCli::new(runner.clone())) as Arc<dyn PveshPort>;
        let fs = Arc::new(FsCli::new(runner.clone())) as Arc<dyn FsPort>;

        Ok(Self {
            pbs,
            zfs,
            lvm,
            block,
            dd,
            pvesh,
            fs,
        })
    }

    #[inline]
    pub fn pbs(&self) -> Arc<dyn PbsPort> {
        self.pbs.clone()
    }
    #[inline]
    pub fn zfs(&self) -> Option<Arc<dyn ZfsPort>> {
        self.zfs.clone()
    }
    #[inline]
    pub fn lvm(&self) -> Option<Arc<dyn LvmPort>> {
        self.lvm.clone()
    }
    #[inline]
    pub fn block(&self) -> Arc<dyn BlockPort> {
        self.block.clone()
    }
    #[inline]
    pub fn dd(&self) -> Arc<dyn DdPort> {
        self.dd.clone()
    }
    #[inline]
    pub fn pvesh(&self) -> Arc<dyn PveshPort> {
        self.pvesh.clone()
    }
    #[inline]
    pub fn fs(&self) -> Arc<dyn FsPort> {
        self.fs.clone()
    }
}

fn ensure_bins_for_cfg(cfg: &Config) -> Result<()> {
    let mut all: BTreeSet<&'static str> = BTreeSet::new();

    for b in pbs::REQ_BINS {
        all.insert(b);
    }
    for b in block::REQ_BINS {
        all.insert(b);
    }
    if cfg.backup.sources.zfs.is_some() {
        for b in zfs::REQ_BINS {
            all.insert(b);
        }
    }
    if cfg.backup.sources.lvmthin.is_some() {
        for b in lvm::REQ_BINS {
            all.insert(b);
        }
    }

    for b in dd::REQ_BINS {
        all.insert(b);
    }
    for b in pvesh::REQ_BINS {
        all.insert(b);
    }
    for b in fs::REQ_BINS {
        all.insert(b);
    }

    let list: Vec<&'static str> = all.into_iter().collect();
    ensure_bins(list)
}
