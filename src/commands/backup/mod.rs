use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};
use std::{any::Any, collections::HashMap, path::PathBuf, sync::Arc};
use tracing as log;

use crate::{
    AppCtx,
    utils::{
        bins::ensure_bins,
        lock::LockGuard,
        process::{CmdSpec, EnvValue, Pipeline, Runner, StdioSpec},
    },
};
use ns::ensure as ns_ensure;

mod lvmthin;
mod ns;
mod zfs;

#[derive(Debug, Args)]
pub struct BackupArgs {
    #[command(subcommand)]
    pub cmd: BackupCmd,
}

impl BackupArgs {
    pub fn run(&self, ctx: &AppCtx) -> Result<()> {
        self.cmd.run(ctx)
    }
}

#[derive(Debug, Subcommand)]
pub enum BackupCmd {
    Run(BackupRunArgs),
    ListArchives(ListArchivesArgs),
}

impl BackupCmd {
    pub fn run(&self, ctx: &AppCtx) -> Result<()> {
        match self {
            BackupCmd::Run(args) => args.run(ctx),
            BackupCmd::ListArchives(args) => args.run(ctx),
        }
    }
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
    pub meta: Option<Arc<dyn Any + Send + Sync>>,
}

impl Volume {
    #[inline]
    pub fn meta<T: 'static>(&self) -> Option<&T> {
        self.meta.as_deref()?.downcast_ref::<T>()
    }
}

trait Provider {
    fn name(&self) -> &'static str;
    fn discover(&self) -> Result<Vec<Volume>>;
    fn prepare(&mut self, volumes: &[Volume], dry_run: bool) -> Result<()>;
}

#[derive(Args, Debug)]
pub struct BackupRunArgs {
    #[arg(long)]
    pub target: Option<String>,

    #[arg(long)]
    pub dry_run: bool,
}

impl BackupRunArgs {
    pub fn run(&self, ctx: &AppCtx) -> Result<()> {
        ensure_bins(["proxmox-backup-client"])?;
        let _lock = LockGuard::try_acquire("pvtool-backup")?;

        let repo = ctx.cfg.pbs.repo(self.target.as_deref())?;
        let ns_opt = ctx.cfg.pbs.ns.as_deref();

        let mut envs: Vec<(String, EnvValue)> = Vec::new();
        if let Some(ref pw) = ctx.cfg.pbs.password {
            envs.push(("PBS_PASSWORD".to_string(), EnvValue::Secret(pw.clone())));
        }

        let mut providers: Vec<Box<dyn Provider>> = Vec::new();
        if ctx.cfg.zfs.is_some() {
            providers.push(Box::new(zfs::ZfsProvider::new(&ctx.cfg, &ctx.runner)));
        }
        if ctx.cfg.lvmthin.is_some() {
            providers.push(Box::new(lvmthin::LvmThinProvider::new(
                &ctx.cfg,
                &ctx.runner,
            )));
        }

        let mut volumes: Vec<Volume> = Vec::new();
        for p in providers.iter_mut() {
            let mut v = p
                .discover()
                .with_context(|| format!("collect from provider {}", p.name()))?;
            volumes.append(&mut v);
        }

        if volumes.is_empty() {
            log::info!("nothing to backup");
            return Ok(());
        }

        ensure_unique_archive_names(&volumes)?;
        log_plan(&volumes, repo, ns_opt, &ctx.cfg.pbs.backup_id);

        if let Some(ns) = ns_opt {
            ns_ensure(repo, ns, &envs, self.dry_run, &ctx.runner)?;
        }

        for p in providers.iter_mut() {
            p.prepare(&volumes, self.dry_run)?;
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

        let cmd = CmdSpec::new("proxmox-backup-client")
            .args(args)
            .envs(envs.clone())
            .stdout(StdioSpec::Inherit)
            .stderr(StdioSpec::Inherit);

        if self.dry_run {
            log::info!("[backup] DRY-RUN: {}", cmd.render());
            return Ok(());
        }

        let ns_disp = ns_opt.unwrap_or("<root>");
        log::info!(
            "[backup] exec -> repo={repo}, ns={ns_disp}, id={}, devices={}",
            ctx.cfg.pbs.backup_id,
            volumes.len()
        );

        ctx.runner
            .run(&Pipeline::new().cmd(cmd))
            .context("run proxmox-backup-client backup")?;

        log::info!("[backup] done");
        Ok(())
    }
}

#[derive(Args, Debug)]
pub struct ListArchivesArgs {
    #[arg(long)]
    pub target: String,
}

impl ListArchivesArgs {
    pub fn run(&self, ctx: &AppCtx) -> Result<()> {
        let _lock = LockGuard::try_acquire("pvtool-backup")?;
        let ns_opt = ctx.cfg.pbs.ns.as_deref();

        let mut providers: Vec<Box<dyn Provider>> = Vec::new();
        if ctx.cfg.zfs.is_some() {
            providers.push(Box::new(zfs::ZfsProvider::new(&ctx.cfg, &ctx.runner)));
        }
        if ctx.cfg.lvmthin.is_some() {
            providers.push(Box::new(lvmthin::LvmThinProvider::new(
                &ctx.cfg,
                &ctx.runner,
            )));
        }

        let mut volumes: Vec<Volume> = Vec::new();
        for p in providers.iter_mut() {
            let mut v = p
                .discover()
                .with_context(|| format!("discover from provider {}", p.name()))?;
            volumes.append(&mut v);
        }

        if volumes.is_empty() {
            log::info!("nothing to backup");
            return Ok(());
        }

        ensure_unique_archive_names(&volumes)?;
        log_plan(&volumes, "<none>", ns_opt, &ctx.cfg.pbs.backup_id);
        Ok(())
    }
}

fn ensure_unique_archive_names(vols: &[Volume]) -> Result<()> {
    let mut seen: HashMap<&str, &str> = HashMap::new();
    for v in vols {
        if let Some(prev) = seen.insert(v.archive.as_str(), v.label.as_str()) {
            bail!(
                "archive name collision: '{}' from '{}' and '{}'",
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
