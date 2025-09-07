pub mod lvmthin;
pub mod zfs;

use anyhow::Result;

use crate::{AppCtx, tooling::PbsSnapshot, volume::Volume};

pub trait Provider {
    fn name(&self) -> &'static str;
    fn collect_restore(
        &mut self,
        archive: Option<&str>,
        all: bool,
        force: bool,
    ) -> Result<Vec<Volume>>;
    fn list_archives(&self, snap: &PbsSnapshot) -> Vec<String>;
}

pub struct ProviderRegistry<'a> {
    ctx: &'a AppCtx,
    snapshot: Option<&'a PbsSnapshot>,
}

impl<'a> ProviderRegistry<'a> {
    pub fn new(ctx: &'a AppCtx, snapshot: Option<&'a PbsSnapshot>) -> Self {
        Self { ctx, snapshot }
    }

    pub fn build(&self) -> Vec<Box<dyn Provider + 'a>> {
        let mut out: Vec<Box<dyn Provider + 'a>> = Vec::new();
        let cfg = &self.ctx.cfg;

        if cfg.zfs.is_some() {
            let zfs_port = self.ctx.tools.zfs().expect("zfs enabled");
            out.push(Box::new(zfs::ZfsRestore::new(
                cfg,
                self.snapshot,
                zfs_port,
                self.ctx.tools.pvesh(),
                self.ctx.tools.fs(),
            )));
        }
        if cfg.lvmthin.is_some() {
            let lvm_port = self.ctx.tools.lvm().expect("lvm enabled");
            out.push(Box::new(lvmthin::LvmthinRestore::new(
                cfg,
                self.snapshot,
                lvm_port,
                self.ctx.tools.pvesh(),
            )));
        }

        out
    }
}
