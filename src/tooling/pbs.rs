use std::{path::Path, sync::Arc};

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::{
    config::Pbs,
    utils::{
        exec_policy,
        process::{CmdSpec, EnvValue, Pipeline, Runner, StdioSpec},
    },
};

pub const REQ_BINS: &[&str] = &["proxmox-backup-client"];

#[derive(Debug, Deserialize)]
pub struct PbsFile {
    pub filename: String,
}

#[derive(Debug, Deserialize)]
pub struct PbsSnapshot {
    #[serde(rename = "backup-id")]
    pub backup_id: String,
    #[serde(rename = "backup-time")]
    pub backup_time: u64,
    pub files: Vec<PbsFile>,
}

#[derive(Debug, Clone, Copy)]
pub struct BackupItem<'a> {
    pub archive: &'a str,
    pub device: &'a Path,
}

pub trait PbsPort: Send + Sync {
    fn snapshots(&self, repo: &str, ns: Option<&str>) -> Result<Vec<PbsSnapshot>>;
    fn ns_exists(&self, repo: &str, ns: &str) -> Result<bool>;
    fn ns_ensure(&self, repo: &str, ns: &str) -> Result<()>;
    fn backup(
        &self,
        repo: &str,
        ns: Option<&str>,
        backup_id: &str,
        keyfile: Option<&Path>,
        items: &[BackupItem<'_>],
    ) -> Result<()>;

    fn restore_to(
        &self,
        repo: &str,
        ns: Option<&str>,
        backup_id: &str,
        archive: &str,
        keyfile: Option<&Path>,
        dd_cmd: crate::utils::process::CmdSpec,
    ) -> Result<()>;
}

type DynRunner = dyn Runner + Send + Sync;

pub struct PbsCli {
    runner: Arc<DynRunner>,
    pbs: Arc<Pbs>,
}

impl PbsCli {
    pub fn new(runner: Arc<DynRunner>, pbs: Arc<Pbs>) -> Self {
        Self { runner, pbs }
    }

    fn pbs_client(&self) -> CmdSpec {
        let mut cmd = CmdSpec::new("proxmox-backup-client");
        if let Some(ref pw) = self.pbs.password {
            cmd = cmd.env("PBS_PASSWORD", EnvValue::Secret(pw.clone()));
        }
        cmd
    }
}

impl PbsPort for PbsCli {
    fn snapshots(&self, repo: &str, ns: Option<&str>) -> Result<Vec<PbsSnapshot>> {
        let mut cmd =
            self.pbs_client()
                .args(["snapshots", "--repository", repo, "--output-format", "json"]);
        if let Some(ns) = ns {
            cmd = cmd.args(["--ns", ns]);
        }

        let out = self
            .runner
            .run_capture(&Pipeline::new().cmd(cmd))
            .context("run proxmox-backup-client snapshots")?;

        let snaps: Vec<PbsSnapshot> =
            serde_json::from_slice(out.as_bytes()).context("parse PBS snapshots json")?;
        Ok(snaps)
    }

    fn ns_exists(&self, repo: &str, ns: &str) -> Result<bool> {
        let cmd = self
            .pbs_client()
            .args(["namespace", "list", "--repository", repo])
            .stdout(StdioSpec::Pipe)
            .stderr(StdioSpec::Null);
        let out = self
            .runner
            .run_capture(&Pipeline::new().cmd(cmd))
            .with_context(|| format!("pbs namespace list on {repo}"))?;
        Ok(out
            .lines()
            .any(|line| line.split_whitespace().any(|tok| tok == ns)))
    }

    fn ns_ensure(&self, repo: &str, ns: &str) -> Result<()> {
        if self.ns_exists(repo, ns)? {
            tracing::debug!("namespace '{ns}' exists on {repo}");
            return Ok(());
        }

        tracing::info!("namespace '{ns}' not found on {repo}, creating…");
        let cmd = self
            .pbs_client()
            .args(["namespace", "create", ns, "--repository", repo])
            .stdout(StdioSpec::Inherit)
            .stderr(StdioSpec::Inherit);
        self.runner
            .run(&Pipeline::new().cmd(cmd))
            .with_context(|| {
                format!("run proxmox-backup-client namespace create '{ns}' on {repo}")
            })?;

        if exec_policy::is_dry_run() {
            return Ok(());
        }
        if self.ns_exists(repo, ns)? {
            Ok(())
        } else {
            anyhow::bail!("namespace '{ns}' still not visible after create on {repo}")
        }
    }

    fn backup(
        &self,
        repo: &str,
        ns: Option<&str>,
        backup_id: &str,
        keyfile: Option<&Path>,
        items: &[BackupItem<'_>],
    ) -> Result<()> {
        let mut cmd = self
            .pbs_client()
            .arg("backup")
            .stdout(StdioSpec::Inherit)
            .stderr(StdioSpec::Inherit);

        for it in items {
            let pair = format!("{}:{}", it.archive, it.device.display());
            cmd = cmd.arg(pair);
        }

        cmd = cmd.arg("--backup-id").arg(backup_id);
        if let Some(ns) = ns {
            cmd = cmd.arg("--ns").arg(ns);
        }
        cmd = cmd.arg("--repository").arg(repo);

        if let Some(kf) = keyfile {
            cmd = cmd.arg("--keyfile").arg(kf.display().to_string());
        }

        self.runner
            .run(&Pipeline::new().cmd(cmd))
            .context("run proxmox-backup-client backup")
    }

    fn restore_to(
        &self,
        repo: &str,
        ns: Option<&str>,
        backup_id: &str,
        archive: &str,
        keyfile: Option<&Path>,
        dd_cmd: crate::utils::process::CmdSpec,
    ) -> Result<()> {
        let mut pbs = self
            .pbs_client()
            .arg("restore")
            .arg(format!("host/{}", backup_id))
            .arg(archive)
            .arg("-");

        if let Some(ns) = ns {
            pbs = pbs.arg("--ns").arg(ns);
        }
        pbs = pbs.arg("--repository").arg(repo);

        if let Some(kf) = keyfile {
            pbs = pbs.arg("--keyfile").arg(kf.display().to_string());
        }

        self.runner
            .run(&Pipeline::new().cmd(pbs).cmd(dd_cmd))
            .with_context(|| format!("restore pipeline for {archive} on repo {repo}"))
    }
}
