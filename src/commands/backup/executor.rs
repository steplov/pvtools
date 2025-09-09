use anyhow::{Context, Result};
use tracing;

use super::providers::ProviderRegistry;
use crate::{
    AppCtx,
    tooling::pbs::BackupItem,
    ui,
    utils::{exec_policy::with_dry_run_enabled, lock::LockGuard},
    volume::{Volume, VolumeSliceExt},
};

pub fn backup(ctx: &AppCtx, target: Option<&str>, dry_run: bool) -> Result<()> {
    let _lock = LockGuard::try_acquire("pvtool-backup")?;

    with_dry_run_enabled(dry_run, || {
        let repo = ctx.cfg.resolve_backup_repo(target)?;
        let ns_opt = ctx.cfg.pbs.ns.as_deref();
        let registry = ProviderRegistry::new(ctx);
        let mut providers = registry.build();
        let mut volumes: Vec<Volume> = Vec::new();

        for p in providers.iter_mut() {
            let mut v = p
                .discover()
                .with_context(|| format!("collect from provider {}", p.name()))?;
            volumes.append(&mut v);
        }

        if volumes.is_empty() {
            tracing::info!("nothing to backup");
            return Ok(());
        }

        volumes.ensure_unique_archive_names()?;

        ui::log_pbs_info(repo, ns_opt, &ctx.cfg.pbs.backup_id, None);
        ui::log_archives(&volumes);

        if let Some(ns) = ns_opt {
            ctx.tools.pbs().ns_ensure(repo, ns)?;
        }

        for p in providers.iter_mut() {
            p.prepare(&volumes)?;
        }

        let keyfile = ctx.cfg.pbs.keyfile.as_deref();
        let items: Vec<BackupItem> = volumes
            .iter()
            .map(|v| BackupItem {
                archive: v.archive.as_str(),
                device: v.device.as_path(),
            })
            .collect();
        ctx.tools
            .pbs()
            .backup(repo, ns_opt, &ctx.cfg.pbs.backup_id, keyfile, &items)?;

        if let Ok(ts) = latest_backup_time(ctx, repo, ns_opt, &ctx.cfg.pbs.backup_id) {
            ui::log_pbs_info(repo, ns_opt, &ctx.cfg.pbs.backup_id, Some(ts));
        } else {
            tracing::info!("Backup finished, but latest snapshot time is not visible yet.");
        }
        tracing::info!("Done");
        Ok(())
    })
}

pub fn list_archives(ctx: &AppCtx) -> Result<()> {
    let _lock = LockGuard::try_acquire("pvtool-backup")?;
    let registry = ProviderRegistry::new(ctx);
    let mut providers = registry.build();
    let mut volumes: Vec<Volume> = Vec::new();

    for p in providers.iter_mut() {
        let mut v = p
            .discover()
            .with_context(|| format!("discover from provider {}", p.name()))?;
        volumes.append(&mut v);
    }

    if volumes.is_empty() {
        tracing::info!("nothing to backup");
        return Ok(());
    }

    volumes.ensure_unique_archive_names()?;

    ui::log_archives(&volumes);

    Ok(())
}

fn latest_backup_time(ctx: &AppCtx, repo: &str, ns: Option<&str>, backup_id: &str) -> Result<u64> {
    let snaps = ctx.tools.pbs().snapshots(repo, ns)?;
    snaps
        .iter()
        .filter(|s| s.backup_id == backup_id)
        .map(|s| s.backup_time)
        .max()
        .context("no snapshot visible after backup with given backup-id")
}
