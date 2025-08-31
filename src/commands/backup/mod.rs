pub mod lvmthin;
mod ns;
pub mod zfs;

use anyhow::{Context, Result, bail};
use clap::Args;
use std::{
    collections::HashMap,
    path::PathBuf,
    process::{Command, Stdio},
};
use tracing as log;

use crate::{
    AppCtx,
    utils::{bins::ensure_bins, lock::LockGuard, shell::sh_quote},
};

#[derive(Args, Debug, Clone)]
pub struct BackupArgs {
    #[arg(long)]
    pub target: Option<String>,

    #[arg(long)]
    pub dry_run: bool,
}

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
}

trait Provider {
    fn name(&self) -> &'static str;
    fn collect(&mut self, dry_run: bool) -> Result<Vec<Volume>>;
}

impl BackupArgs {
    pub fn run(&self, ctx: &AppCtx) -> Result<()> {
        ensure_bins(["proxmox-backup-client"])?;

        let _lock = LockGuard::try_acquire("pvtool-backup")?;

        let repo = ctx.cfg.pbs.repo(self.target.as_deref())?;
        let ns_opt = ctx.cfg.pbs.ns.as_deref();

        let mut cmd_env = Vec::<(String, String)>::new();
        if let Some(ref pw) = ctx.cfg.pbs.password {
            cmd_env.push(("PBS_PASSWORD".to_string(), pw.clone()));
        }

        let mut providers: Vec<Box<dyn Provider>> = Vec::new();

        if ctx.cfg.zfs.is_some() {
            providers.push(Box::new(zfs::ZfsProvider::new(&ctx.cfg)));
        }

        if ctx.cfg.lvmthin.is_some() {
            providers.push(Box::new(lvmthin::LvmThinProvider::new(&ctx.cfg)));
        }

        let mut volumes: Vec<Volume> = Vec::new();
        for p in providers.iter_mut() {
            let mut v = p
                .collect(self.dry_run)
                .with_context(|| format!("collect from provider {}", p.name()))?;
            volumes.append(&mut v);
        }

        if volumes.is_empty() {
            log::info!("nothing to backup (no matching volumes from any provider)");
            return Ok(());
        }

        ensure_unique_archive_names(&volumes)?;
        log_plan(&volumes, repo, ns_opt, &ctx.cfg.pbs.backup_id);

        if let Some(ns) = ns_opt {
            ns::ensure(repo, ns, &cmd_env, self.dry_run)?;
        }

        let mut args: Vec<String> = Vec::new();
        args.push("backup".to_string());
        for v in &volumes {
            args.push(format!("{}:{}", v.archive, v.device.display()));
        }
        args.push("--backup-id".to_string());
        args.push(ctx.cfg.pbs.backup_id.clone());
        if let Some(ns) = ns_opt {
            args.push("--ns".to_string());
            args.push(ns.to_string());
        }
        args.push("--repository".to_string());
        args.push(repo.to_string());
        if let Some(ref kf) = ctx.cfg.pbs.keyfile {
            args.push("--keyfile".to_string());
            args.push(kf.display().to_string());
        }

        if self.dry_run {
            let mut prefix = String::new();
            if ctx.cfg.pbs.password.is_some() {
                prefix.push_str("PBS_PASSWORD=<redacted> ");
            }
            let cmdline = format!(
                "{}proxmox-backup-client {}",
                prefix,
                args.iter()
                    .map(|a| sh_quote(a))
                    .collect::<Vec<_>>()
                    .join(" ")
            );
            log::info!("[backup] DRY-RUN: {}", cmdline);
            return Ok(());
        }

        let mut cmd = Command::new("proxmox-backup-client");
        for (k, v) in &cmd_env {
            cmd.env(k, v);
        }
        cmd.args(&args);
        cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit());

        let ns_disp = ns_opt.unwrap_or("<root>");
        log::info!(
            "[backup] exec -> repo={repo}, ns={ns_disp}, id={}, devices={}",
            ctx.cfg.pbs.backup_id,
            volumes.len()
        );

        let status = cmd.status().context("run proxmox-backup-client backup")?;
        if !status.success() {
            bail!("proxmox-backup-client exited with {}", status);
        }

        log::info!("[backup] done");
        Ok(())
    }
}

fn ensure_unique_archive_names(vols: &[Volume]) -> Result<()> {
    let mut seen: HashMap<&str, &str> = HashMap::new(); // archive -> label
    for v in vols {
        if let Some(prev) = seen.insert(v.archive.as_str(), v.label.as_str()) {
            bail!(
                "archive name collision: '{}' from '{}' and '{}'. \
                 Check provider naming logic (should be unique per source).",
                v.archive,
                prev,
                v.label
            );
        }
    }
    Ok(())
}

fn log_plan(vols: &[Volume], repo: &str, ns: Option<&str>, backup_id: &str) {
    let ns_disp = ns.unwrap_or("<root>");
    log::info!(
        "[backup] plan -> repo={repo}, ns={ns_disp}, id={backup_id}, items={}",
        vols.len()
    );
    for v in vols {
        log::info!(
            "[backup]   {} -> host/{}/{}",
            v.map_src,
            backup_id,
            v.archive
        );
    }
}
