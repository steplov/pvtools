use anyhow::Result;
use std::{any::Any, path::PathBuf, sync::Arc};

#[derive(Debug, Clone)]
pub struct Volume {
    /// Final PBS archive file name (must end with ".img")
    pub archive: String,
    /// Block device to read from (e.g. /dev/zvol/... or /dev/<vg>/<snap>)
    pub device: PathBuf,
    /// Provider-specific label for debugging (e.g. "zfs:tank/...").
    pub label: String,
    /// Human-friendly source path to print in the mapping (left side).
    /// For example: "/tank/…/vm-9999-…" for ZFS, or "/dev/<vg>/<lv>" for LVM-thin.
    pub map_src: String,
    pub meta: Option<Arc<dyn Any + Send + Sync>>,
}

impl Volume {
    #[inline]
    pub fn meta<T: 'static>(&self) -> Option<&T> {
        self.meta.as_deref()?.downcast_ref::<T>()
    }
}

pub trait Provider {
    fn name(&self) -> &'static str;
    fn discover(&self) -> Result<Vec<Volume>>;
    fn prepare(&mut self, volumes: &[Volume], dry_run: bool) -> Result<()>;
}
