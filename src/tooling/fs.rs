use std::{path::Path, sync::Arc};

use anyhow::{Context, Result};

use crate::utils::process::{CmdSpec, Pipeline, Runner, StdioSpec};

pub const REQ_BINS: &[&str] = &["mkdir", "truncate"];

type DynRunner = dyn Runner + Send + Sync;

pub trait FsPort: Send + Sync {
    fn ensure_dir(&self, dir: &Path) -> Result<()>;
    fn ensure_parent_dir(&self, path: &Path) -> Result<()>;
    fn create_sparse_file(&self, path: &Path, size_bytes: u64) -> Result<()>;
}

pub struct FsCli {
    runner: Arc<DynRunner>,
}

impl FsCli {
    pub fn new(runner: Arc<DynRunner>) -> Self {
        Self { runner }
    }

    #[inline]
    fn mkdir_p(&self, dir: &Path) -> CmdSpec {
        CmdSpec::new("mkdir")
            .arg("-p")
            .arg(dir.display().to_string())
            .stdout(StdioSpec::Null)
            .stderr(StdioSpec::Inherit)
    }

    #[inline]
    fn truncate(&self, path: &Path, size_bytes: u64) -> CmdSpec {
        CmdSpec::new("truncate")
            .arg("-s")
            .arg(size_bytes.to_string())
            .arg(path.display().to_string())
            .stdout(StdioSpec::Null)
            .stderr(StdioSpec::Inherit)
    }
}

impl FsPort for FsCli {
    fn ensure_dir(&self, dir: &Path) -> Result<()> {
        let cmd = self.mkdir_p(dir);
        self.runner
            .run(&Pipeline::new().cmd(cmd))
            .with_context(|| format!("mkdir -p {}", dir.display()))
    }

    fn ensure_parent_dir(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            self.ensure_dir(parent)
        } else {
            Ok(())
        }
    }

    fn create_sparse_file(&self, path: &Path, size_bytes: u64) -> Result<()> {
        self.ensure_parent_dir(path)?;

        let cmd = self.truncate(path, size_bytes);
        self.runner
            .run(&Pipeline::new().cmd(cmd))
            .with_context(|| format!("truncate -s {} {}", size_bytes, path.display()))
    }
}
