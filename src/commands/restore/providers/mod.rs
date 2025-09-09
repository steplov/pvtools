pub mod lvmthin;
pub mod zfs;

use std::sync::Arc;

use anyhow::Result;

use crate::{
    AppCtx, commands::restore::matcher::RestoreMatcher, config::RestoreTarget,
    tooling::pbs::PbsSnapshot, volume::Volume,
};

pub trait Provider {
    fn name(&self) -> &'static str;
    fn collect_restore(&mut self, archive: Option<&str>, all: bool) -> Result<Vec<Volume>>;
    fn list_archives(&self, snap: &PbsSnapshot) -> Vec<String>;
}

pub struct ProviderRegistry<'a> {
    ctx: &'a AppCtx,
    snapshot: Option<&'a PbsSnapshot>,
    matcher: Arc<RestoreMatcher>,
}

impl<'a> ProviderRegistry<'a> {
    pub fn new(ctx: &'a AppCtx, snapshot: Option<&'a PbsSnapshot>) -> Self {
        let matcher = Arc::new(RestoreMatcher::new(&ctx.cfg).expect("restore matcher"));
        Self {
            ctx,
            snapshot,
            matcher,
        }
    }

    pub fn build(&self) -> Vec<Box<dyn Provider + 'a>> {
        let mut out: Vec<Box<dyn Provider + 'a>> = Vec::new();
        for (tname, tgt) in &self.ctx.cfg.restore.targets {
            match tgt {
                RestoreTarget::Zfs { root } => {
                    let zfs_port = self.ctx.tools.zfs().expect("zfs enabled");
                    let pvesh = self.ctx.tools.pvesh();
                    let fs = self.ctx.tools.fs();
                    out.push(Box::new(zfs::ZfsRestore::new(
                        self.snapshot,
                        zfs_port,
                        pvesh,
                        fs,
                        self.matcher.clone(),
                        root.clone(),
                        tname.clone(),
                    )));
                }
                RestoreTarget::LvmThin { vg, thinpool } => {
                    let lvm_port = self.ctx.tools.lvm().expect("lvm enabled");
                    let pvesh = self.ctx.tools.pvesh();
                    let tp = thinpool
                        .clone()
                        .expect("[lvmthin target] thinpool is required");
                    out.push(Box::new(lvmthin::LvmthinRestore::new(
                        self.snapshot,
                        lvm_port,
                        pvesh,
                        self.matcher.clone(),
                        vg.clone(),
                        tp,
                        tname.clone(),
                    )));
                }
            }
        }

        out
    }
}
