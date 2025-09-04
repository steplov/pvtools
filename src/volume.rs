use std::{
    any::Any,
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Result, bail};

#[derive(Debug, Clone)]
pub struct Volume {
    /// Proxmox storage ID (e.g. "local-zfs", "local-lvm")
    pub storage: String,
    /// Disk name without path (e.g. "vm-123.raw")
    pub disk: String,
    /// Final PBS archive file name (must end with ".img")
    pub archive: String,
    /// Block device to read from/write to (e.g. /dev/zvol/... or /dev/<vg>/<snap>)
    pub device: PathBuf,
    pub meta: Option<Arc<dyn Any + Send + Sync>>,
}

impl Volume {
    #[inline]
    pub fn meta<T: 'static>(&self) -> Option<&T> {
        self.meta.as_deref()?.downcast_ref::<T>()
    }
}

pub trait VolumeSliceExt {
    fn ensure_unique_targets(&self) -> Result<()>;
    fn ensure_unique_archive_names(&self) -> Result<()>;
}

impl VolumeSliceExt for [Volume] {
    fn ensure_unique_targets(&self) -> Result<()> {
        let mut seen: HashSet<&Path> = HashSet::new();
        for v in self {
            let p = v.device.as_path();
            if !seen.insert(p) {
                bail!("target collision: '{}'", v.device.display());
            }
        }
        Ok(())
    }

    fn ensure_unique_archive_names(&self) -> Result<()> {
        let mut seen: HashSet<&str> = HashSet::new();
        for v in self {
            if !seen.insert(v.archive.as_str()) {
                bail!("archive name collision: '{}'", v.archive);
            }
        }
        Ok(())
    }
}
