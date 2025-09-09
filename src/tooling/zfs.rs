use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Result};

use crate::utils::process::{CmdSpec, Pipeline, Runner, StdioSpec};

pub const REQ_BINS: &[&str] = &["zfs"];

pub trait ZfsPort: Send + Sync {
    fn list_volumes(&self, pool: &str) -> Result<Vec<ZfsVolume>>;
    fn guid_map(&self, pool: &str) -> Result<HashMap<String, String>>;
    fn snapshot(&self, snap: &str) -> Result<()>;
    fn clone_readonly_dev(&self, snap: &str, clone: &str) -> Result<()>;
    fn destroy_recursive(&self, target: &str) -> Result<()>;
    fn assert_dataset_exists(&self, dataset: &str) -> Result<()>;
    fn dataset_mountpoint(&self, dataset: &str) -> Result<Option<String>>;
    fn create_zvol(&self, dataset: &str, size_bytes: u64) -> anyhow::Result<()>;
}

type DynRunner = dyn Runner + Send + Sync;

pub struct ZfsCli {
    runner: Arc<DynRunner>,
}

impl ZfsCli {
    pub fn new(runner: Arc<DynRunner>) -> Self {
        Self { runner }
    }

    #[inline]
    fn zfs(&self) -> CmdSpec {
        CmdSpec::new("zfs")
    }
}

#[derive(Debug, Clone)]
pub struct ZfsVolume {
    pub name: String,
    pub origin: Option<String>,
}

impl ZfsPort for ZfsCli {
    fn list_volumes(&self, pool: &str) -> Result<Vec<ZfsVolume>> {
        let cmd = self
            .zfs()
            .args([
                "list",
                "-H",
                "-t",
                "volume",
                "-o",
                "name,origin",
                "-r",
                pool,
            ])
            .stdout(StdioSpec::Pipe);

        let out_txt = self
            .runner
            .run_capture(&Pipeline::new().cmd(cmd))
            .with_context(|| format!("zfs list for pool {pool}"))?;

        let mut volumes: Vec<ZfsVolume> = Vec::new();

        for line in out_txt.lines() {
            let mut it = line.split_whitespace();
            let name = match it.next() {
                Some(x) => x,
                None => continue,
            };
            let origin = match it.next() {
                Some(x) => x,
                None => continue,
            };

            volumes.push(ZfsVolume {
                name: name.to_string(),
                origin: if origin == "-" {
                    None
                } else {
                    Some(origin.to_string())
                },
            })
        }

        Ok(volumes)
    }

    fn guid_map(&self, pool: &str) -> Result<HashMap<String, String>> {
        let cmd = self
            .zfs()
            .args(["get", "-H", "-o", "name,value", "guid", "-r", pool])
            .stdout(StdioSpec::Pipe)
            .stderr(StdioSpec::Null);

        let out = self
            .runner
            .run_capture(&Pipeline::new().cmd(cmd))
            .with_context(|| format!("zfs get guid -r {pool}"))?;

        let mut map = HashMap::new();
        for line in out.lines() {
            let mut it = line.split_whitespace();
            if let (Some(ds), Some(guid_str)) = (it.next(), it.next()) {
                let n: u128 = guid_str.trim().parse().unwrap_or(0);
                let hex = format!("{n:x}");
                let short = hex.chars().take(8).collect::<String>();
                map.insert(ds.to_string(), short);
            }
        }
        Ok(map)
    }

    fn snapshot(&self, snap: &str) -> Result<()> {
        let cmd = self
            .zfs()
            .args(["snapshot", snap])
            .stderr(StdioSpec::Inherit);
        self.runner
            .run(&Pipeline::new().cmd(cmd))
            .with_context(|| format!("zfs snapshot {snap}"))
    }

    fn clone_readonly_dev(&self, snap: &str, clone: &str) -> Result<()> {
        let cmd = self
            .zfs()
            .args([
                "clone",
                "-o",
                "readonly=on",
                "-o",
                "volmode=dev",
                snap,
                clone,
            ])
            .stderr(StdioSpec::Inherit);
        self.runner
            .run(&Pipeline::new().cmd(cmd))
            .with_context(|| format!("zfs clone {snap} -> {clone}"))
    }

    fn destroy_recursive(&self, target: &str) -> Result<()> {
        let cmd = self
            .zfs()
            .args(["destroy", "-r", target])
            .stderr(StdioSpec::Inherit);

        self.runner
            .run(&Pipeline::new().cmd(cmd))
            .with_context(|| format!("zfs destroy -r {target}"))
    }

    fn assert_dataset_exists(&self, dataset: &str) -> Result<()> {
        let cmd = self
            .zfs()
            .args(["list", "-H", "-o", "name", dataset])
            .stdout(StdioSpec::Null)
            .stderr(StdioSpec::Null);

        self.runner
            .run(&Pipeline::new().cmd(cmd))
            .with_context(|| format!("zfs list {dataset}"))
    }

    fn dataset_mountpoint(&self, dataset: &str) -> Result<Option<String>> {
        let cmd = self
            .zfs()
            .args(["get", "-H", "-o", "value", "mountpoint", dataset])
            .stdout(StdioSpec::Pipe)
            .stderr(StdioSpec::Null);

        let out = self
            .runner
            .run_capture(&Pipeline::new().cmd(cmd))
            .with_context(|| format!("zfs get mountpoint {dataset}"))?;

        let mp = out.trim();

        Ok(match mp {
            "-" | "none" => None,
            path => Some(path.to_string()),
        })
    }

    fn create_zvol(&self, dataset: &str, size_bytes: u64) -> Result<()> {
        let cmd = self
            .zfs()
            .args(["create", "-V", &size_bytes.to_string(), dataset]);
        self.runner
            .run(&Pipeline::new().cmd(cmd))
            .with_context(|| format!("zfs create -V {} {}", size_bytes, dataset))
    }
}
