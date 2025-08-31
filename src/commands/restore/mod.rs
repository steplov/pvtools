pub mod lvmthin;
pub mod zfs;

use anyhow::{Context, Result, bail};
use clap::Args;
use serde::Deserialize;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};
use tracing as log;

use crate::{
    AppCtx,
    utils::{
        bins::ensure_bins,
        lock::LockGuard,
        process::{CmdSpec, Pipeline, Runner},
    },
};

#[derive(Debug, Clone)]
pub enum RestorePoint {
    Latest,
    At(u64),
}

#[derive(Args, Debug, Clone)]
pub struct RestoreArgs {
    #[arg(long)]
    pub source: Option<String>,
    pub archive: Option<String>,
    #[arg(long, default_value = "latest")]
    pub timestamp: String,
    #[arg(long)]
    pub all: bool,
    #[arg(long)]
    pub force: bool,
    #[arg(long)]
    pub dry_run: bool,
    #[arg(long)]
    pub list: bool,
}

#[derive(Debug, Clone)]
pub struct RestoreItem {
    pub archive: String,
    pub target: PathBuf,
    pub label: String,
}

pub trait Provider {
    fn name(&self) -> &'static str;
    fn collect_restore(
        &mut self,
        archive: Option<&str>,
        all: bool,
        force: bool,
    ) -> Result<Vec<RestoreItem>>;
    fn list_archives(&self, snap: &PbsSnapshot) -> Vec<String>;
}

struct RestorePlanCmd {
    pbs_args: Vec<String>,
    dd_args: Vec<String>,
}

fn build_restore_cmd(
    snap: &PbsSnapshot,
    archive: &str,
    target: &Path,
    repo: &str,
    ns: Option<&str>,
    keyfile: Option<&PathBuf>,
) -> RestorePlanCmd {
    let mut pbs = vec![
        "restore".to_string(),
        format!("host/{}", snap.backup_id),
        archive.to_string(),
        "-".to_string(), // stream to stdout
    ];
    if let Some(ns) = ns {
        pbs.push("--ns".to_string());
        pbs.push(ns.to_string());
    }
    pbs.push("--repository".to_string());
    pbs.push(repo.to_string());
    if let Some(kf) = keyfile {
        pbs.push("--keyfile".to_string());
        pbs.push(kf.display().to_string());
    }

    let dd = vec![
        format!("of={}", target.display()),
        "bs=4M".to_string(),
        "conv=notrunc".to_string(),
        "oflag=direct".to_string(),
        "status=progress".to_string(),
    ];

    RestorePlanCmd {
        pbs_args: pbs,
        dd_args: dd,
    }
}

impl RestoreArgs {
    pub fn run(&self, ctx: &AppCtx) -> Result<()> {
        ensure_bins(["proxmox-backup-client", "dd"])?;
        let _lock = LockGuard::try_acquire("pvtool-restore")?;

        let repo = ctx.cfg.pbs.repo(self.source.as_deref())?;
        let ns_opt = ctx.cfg.pbs.ns.as_deref();

        let point = if self.timestamp == "latest" {
            RestorePoint::Latest
        } else {
            let ts: u64 = self
                .timestamp
                .parse()
                .context("invalid --timestamp, expected u64 or 'latest'")?;
            RestorePoint::At(ts)
        };

        let snaps = list_snapshots(repo, ns_opt, &ctx.runner)?;
        if snaps.is_empty() {
            bail!("no snapshots found in repo {repo}");
        }

        let snap = match point {
            RestorePoint::Latest => pick_latest(&snaps, &ctx.cfg.pbs.backup_id)
                .context("no snapshots found for backup-id")?,
            RestorePoint::At(ts) => pick_by_time(&snaps, &ctx.cfg.pbs.backup_id, ts)
                .context("no matching snapshot found before given time")?,
        };

        let mut providers: Vec<Box<dyn Provider>> = Vec::new();
        if ctx.cfg.zfs.is_some() {
            providers.push(Box::new(zfs::ZfsRestore::new(
                &ctx.cfg,
                Some(snap),
                &ctx.runner,
            )));
        }
        if ctx.cfg.lvmthin.is_some() {
            providers.push(Box::new(lvmthin::LvmthinRestore::new(
                &ctx.cfg,
                Some(snap),
                &ctx.runner,
            )));
        }

        if self.list {
            log::info!(
                "[restore] available archives in snapshot {} at {}:",
                snap.backup_id,
                snap.backup_time
            );
            for p in &providers {
                let archives = p.list_archives(snap);
                if !archives.is_empty() {
                    log::info!("  [{}]", p.name());
                    for a in archives {
                        log::info!("    {}", a);
                    }
                }
            }
            return Ok(());
        }

        log::info!(
            "[restore] selected snapshot backup-id={} time={} files={}",
            snap.backup_id,
            snap.backup_time,
            snap.files.len()
        );

        let mut items: Vec<RestoreItem> = Vec::new();
        for p in providers.iter_mut() {
            let mut r = p
                .collect_restore(self.archive.as_deref(), self.all, self.force)
                .with_context(|| format!("collect restore plan from provider {}", p.name()))?;
            items.append(&mut r);
        }

        if items.is_empty() {
            log::info!("nothing to restore (no matching archives from any provider)");
            return Ok(());
        }

        ensure_unique_targets(&items)?;
        log_plan(&items, repo, ns_opt);

        for i in &items {
            let plan = build_restore_cmd(
                snap,
                &i.archive,
                &i.target,
                repo,
                ns_opt,
                ctx.cfg.pbs.keyfile.as_ref(),
            );

            let cmd_pbs = CmdSpec::new("proxmox-backup-client").args(plan.pbs_args.clone());
            let cmd_dd = CmdSpec::new("dd").args(plan.dd_args.clone());

            if self.dry_run {
                log::info!(
                    "[restore] DRY-RUN: {} | {}",
                    cmd_pbs.render(),
                    cmd_dd.render()
                );
                continue;
            }

            ctx.runner
                .run(&Pipeline::new().cmd(cmd_pbs).cmd(cmd_dd))
                .with_context(|| format!("restore pipeline for {}", i.archive))?;
        }

        log::info!("[restore] done");
        Ok(())
    }
}

fn ensure_unique_targets(items: &[RestoreItem]) -> Result<()> {
    let mut seen: HashMap<String, String> = HashMap::new();
    for i in items {
        let tgt = i.target.display().to_string();
        if let Some(prev) = seen.insert(tgt.clone(), i.label.clone()) {
            bail!(
                "target collision: '{}' from '{}' and '{}'",
                tgt,
                prev,
                i.label
            );
        }
    }
    Ok(())
}

fn log_plan(items: &[RestoreItem], repo: &str, ns: Option<&str>) {
    let ns_disp = ns.unwrap_or("<root>");
    log::info!(
        "[restore] plan -> repo={repo}, ns={ns_disp}, items={}",
        items.len()
    );
    for i in items {
        log::info!("[restore]   {} -> {}", i.archive, i.target.display());
    }
}

#[derive(Deserialize)]
pub struct PbsSnapshot {
    #[serde(rename = "backup-id")]
    pub backup_id: String,
    #[serde(rename = "backup-time")]
    pub backup_time: u64,
    pub files: Vec<PbsFile>,
}

#[derive(Deserialize)]
pub struct PbsFile {
    pub filename: String,
}

pub fn list_snapshots(
    repo: &str,
    ns: Option<&str>,
    runner: &dyn Runner,
) -> Result<Vec<PbsSnapshot>> {
    let mut cmd = CmdSpec::new("proxmox-backup-client").args([
        "snapshots",
        "--repository",
        repo,
        "--output-format",
        "json",
    ]);
    if let Some(ns) = ns {
        cmd = cmd.args(["--ns", ns]);
    }

    let out = runner
        .run_capture(&Pipeline::new().cmd(cmd))
        .context("run proxmox-backup-client snapshots")?;

    let snaps: Vec<PbsSnapshot> =
        serde_json::from_slice(out.as_bytes()).context("parse PBS snapshots json")?;
    Ok(snaps)
}

pub fn pick_latest<'a>(snaps: &'a [PbsSnapshot], backup_id: &str) -> Option<&'a PbsSnapshot> {
    snaps
        .iter()
        .filter(|s| s.backup_id == backup_id)
        .max_by_key(|s| s.backup_time)
}

pub fn pick_by_time<'a>(
    snaps: &'a [PbsSnapshot],
    backup_id: &str,
    ts: u64,
) -> Option<&'a PbsSnapshot> {
    snaps
        .iter()
        .filter(|s| s.backup_id == backup_id && s.backup_time <= ts)
        .max_by_key(|s| s.backup_time)
}
