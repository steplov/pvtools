pub mod lvmthin;
pub mod zfs;

use anyhow::Result;

use crate::{AppCtx, volume::Volume};

pub trait Provider {
    fn name(&self) -> &'static str;
    fn discover(&self) -> Result<Vec<Volume>>;
    fn prepare(&mut self, volumes: &[Volume]) -> Result<()>;
}

pub struct ProviderRegistry<'a> {
    ctx: &'a AppCtx,
}

impl<'a> ProviderRegistry<'a> {
    pub fn new(ctx: &'a AppCtx) -> Self {
        Self { ctx }
    }

    pub fn build(&self) -> Vec<Box<dyn Provider + 'a>> {
        let mut out: Vec<Box<dyn Provider + 'a>> = Vec::new();
        let cfg = &self.ctx.cfg;

        if cfg.zfs.is_some() {
            let zfs_port = self.ctx.tools.zfs().expect("zfs enabled");

            out.push(Box::new(zfs::ZfsProvider::new(
                cfg,
                zfs_port,
                self.ctx.tools.block(),
                self.ctx.tools.pvesh(),
            )));
        }
        if cfg.lvmthin.is_some() {
            let lvm_port = self.ctx.tools.lvm().expect("lvm enabled");

            out.push(Box::new(lvmthin::LvmThinProvider::new(
                cfg,
                lvm_port,
                self.ctx.tools.block(),
                self.ctx.tools.pvesh(),
            )));
        }

        out
    }
}
