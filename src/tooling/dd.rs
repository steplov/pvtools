use std::path::Path;

use crate::utils::process::CmdSpec;

pub const REQ_BINS: &[&str] = &["dd"];

#[derive(Debug, Clone)]
pub struct DdOpts {
    pub bs: Option<&'static str>,
    pub conv_notrunc: bool,
    pub oflag_direct: bool,
    pub status_progress: bool,
}

impl Default for DdOpts {
    fn default() -> Self {
        Self {
            bs: Some("4M"),
            conv_notrunc: true,
            oflag_direct: true,
            status_progress: true,
        }
    }
}

pub trait DdPort: Send + Sync {
    fn to_file_cmd(&self, target: &Path, opts: &DdOpts) -> CmdSpec;
}

pub struct DdCli;

impl DdCli {
    pub fn new() -> Self {
        Self
    }
}

impl DdPort for DdCli {
    fn to_file_cmd(&self, target: &Path, opts: &DdOpts) -> CmdSpec {
        let mut cmd = CmdSpec::new("dd").arg(format!("of={}", target.display()));
        if let Some(bs) = opts.bs {
            cmd = cmd.arg(format!("bs={}", bs));
        }
        if opts.conv_notrunc {
            cmd = cmd.arg("conv=notrunc");
        }
        if opts.oflag_direct {
            cmd = cmd.arg("oflag=direct");
        }
        if opts.status_progress {
            cmd = cmd.arg("status=progress");
        }
        cmd
    }
}
