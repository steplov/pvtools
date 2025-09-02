use super::providers::{lvmthin, zfs};
use anyhow::{Context, Result, bail};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};
use tracing as log;

use crate::{
    AppCtx,
    utils::{
        bins::ensure_bins,
        lock::LockGuard,
        process::{CmdSpec, Pipeline},
        time::fmt_utc,
        time::parse_rfc3339_to_unix,
    },
};

use super::types::{PbsSnapshot, Provider, RestoreItem, RestorePoint};

pub struct ListSnapshotsOpts {
    pub source: Option<String>,
}

impl From<&super::ListSnapshotsArgs> for ListSnapshotsOpts {
    fn from(value: &super::ListSnapshotsArgs) -> Self {
        Self {
            source: value.source.clone(),
        }
    }
}

pub struct ListArchivesOpts {
    pub source: Option<String>,
    pub snapshot: RestorePoint,
}

impl TryFrom<&super::ListArchivesArgs> for ListArchivesOpts {
    type Error = anyhow::Error;
    fn try_from(value: &super::ListArchivesArgs) -> Result<Self> {
        let snapshot = parse_point(&value.snapshot)?;
        Ok(Self {
            source: value.source.clone(),
            snapshot,
        })
    }
}

pub struct RunOpts {
    pub source: Option<String>,
    pub snapshot: RestorePoint,
    pub archives: Vec<String>,
    pub all: bool,
    pub force: bool,
    pub dry_run: bool,
}

impl TryFrom<&super::RestoreRunArgs> for RunOpts {
    type Error = anyhow::Error;
    fn try_from(value: &super::RestoreRunArgs) -> Result<Self> {
        let snapshot = parse_point(&value.snapshot)?;
        Ok(Self {
            source: value.source.clone(),
            snapshot,
            archives: value.archives.clone(),
            all: value.all,
            force: value.force,
            dry_run: value.dry_run,
        })
    }
}

pub fn list_snapshots_cmd(ctx: &AppCtx, opts: ListSnapshotsOpts) -> Result<()> {
    ensure_bins(["proxmox-backup-client"])?;

    let repo = ctx.cfg.pbs.repo(opts.source.as_deref())?;
    let ns_opt = ctx.cfg.pbs.ns.as_deref();

    let snaps = list_snapshots(repo, ns_opt, ctx)?;
    if snaps.is_empty() {
        log::info!("no snapshots found in repo {repo}");
        return Ok(());
    }

    log::info!(
        "[restore] snapshots in repo {repo}, ns={}:",
        ns_opt.unwrap_or("<root>")
    );

    let mut list: Vec<&PbsSnapshot> = snaps
        .iter()
        .filter(|s| s.backup_id == ctx.cfg.pbs.backup_id)
        .collect();
    list.sort_by_key(|s| s.backup_time);

    for s in list {
        let when = fmt_utc(s.backup_time)?;
        log::info!("  time={} files={}", &when, s.files.len());
    }
    Ok(())
}

pub fn list_archives(ctx: &AppCtx, opts: ListArchivesOpts) -> Result<()> {
    ensure_bins(["proxmox-backup-client"])?;
    let repo = ctx.cfg.pbs.repo(opts.source.as_deref())?;
    let ns_opt = ctx.cfg.pbs.ns.as_deref();
    let point = &opts.snapshot;
    let runner = ctx.runner.as_ref();

    let snaps = list_snapshots(repo, ns_opt, ctx)?;
    if snaps.is_empty() {
        bail!("no snapshots found in repo {repo}");
    }
    let snap = match point {
        RestorePoint::Latest => pick_latest(&snaps, &ctx.cfg.pbs.backup_id)
            .context("no snapshots found for backup-id")?,
        RestorePoint::At(ts) => pick_by_time(&snaps, &ctx.cfg.pbs.backup_id, *ts)
            .context("no matching snapshot found before given time")?,
    };

    let mut providers: Vec<Box<dyn Provider>> = Vec::new();
    if ctx.cfg.zfs.is_some() {
        providers.push(Box::new(zfs::ZfsRestore::new(&ctx.cfg, Some(snap), runner)));
    }
    if ctx.cfg.lvmthin.is_some() {
        providers.push(Box::new(lvmthin::LvmthinRestore::new(
            &ctx.cfg,
            Some(snap),
            runner,
        )));
    }

    log::info!(
        "[restore] available archives in snapshot backup-id={} time={}:",
        snap.backup_id,
        snap.backup_time
    );
    for p in providers {
        let a = p.list_archives(snap);
        if a.is_empty() {
            continue;
        }
        log::info!("  [{}]", p.name());
        for f in a {
            log::info!("    {}", f);
        }
    }
    Ok(())
}

pub fn restore_run(ctx: &AppCtx, opts: RunOpts) -> Result<()> {
    ensure_bins(["proxmox-backup-client", "dd"])?;
    let _lock = LockGuard::try_acquire("pvtool-restore")?;
    let repo = ctx.cfg.pbs.repo(opts.source.as_deref())?;
    let ns_opt = ctx.cfg.pbs.ns.as_deref();
    let point = &opts.snapshot;
    let runner = ctx.runner.as_ref();
    let snaps = list_snapshots(repo, ns_opt, ctx)?;
    if snaps.is_empty() {
        bail!("no snapshots found in repo {repo}");
    }

    let snap = match point {
        RestorePoint::Latest => pick_latest(&snaps, &ctx.cfg.pbs.backup_id)
            .context("no snapshots found for backup-id")?,
        RestorePoint::At(ts) => pick_by_time(&snaps, &ctx.cfg.pbs.backup_id, *ts)
            .context("no matching snapshot found before given time")?,
    };

    let mut providers: Vec<Box<dyn Provider>> = Vec::new();
    if ctx.cfg.zfs.is_some() {
        providers.push(Box::new(zfs::ZfsRestore::new(&ctx.cfg, Some(snap), runner)));
    }
    if ctx.cfg.lvmthin.is_some() {
        providers.push(Box::new(lvmthin::LvmthinRestore::new(
            &ctx.cfg,
            Some(snap),
            runner,
        )));
    }

    let mut available: Vec<String> = Vec::new();
    for p in providers.iter_mut() {
        let mut a = p.list_archives(snap);
        available.append(&mut a);
    }

    let selected_archives: Vec<String> =
        select_archives_exact_from(&available, &opts.archives, opts.all)?;

    if selected_archives.is_empty() {
        bail!("nothing to restore: specify --all or at least one --archive");
    }

    log::info!(
        "[restore] selected snapshot backup-id={} time={}",
        snap.backup_id,
        snap.backup_time
    );

    let mut items: Vec<RestoreItem> = Vec::new();
    for p in providers.iter_mut() {
        if opts.all {
            let mut r = p
                .collect_restore(None, true, opts.force)
                .with_context(|| format!("collect restore plan from provider {}", p.name()))?;
            items.append(&mut r);
        } else {
            for a in &selected_archives {
                let mut r = p
                    .collect_restore(Some(a.as_str()), opts.all, opts.force)
                    .with_context(|| format!("collect restore plan from provider {}", p.name()))?;
                items.append(&mut r);
            }
        }
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

        if opts.dry_run {
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

fn list_snapshots(repo: &str, ns: Option<&str>, ctx: &AppCtx) -> Result<Vec<PbsSnapshot>> {
        let mut cmd = CmdSpec::new("proxmox-backup-client").args([
        "snapshots",
        "--repository",
        repo,
        "--output-format",
        "json",
    ]);
    let runner = ctx.runner.as_ref();

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

fn parse_point(s: &str) -> Result<RestorePoint> {
    if s == "latest" {
        return Ok(RestorePoint::Latest);
    }
    if let Ok(ts) = s.parse::<u64>() {
        return Ok(RestorePoint::At(ts));
    }
    let ts = parse_rfc3339_to_unix(s)?;
    Ok(RestorePoint::At(ts))
}

fn pick_latest<'a>(snaps: &'a [PbsSnapshot], backup_id: &str) -> Option<&'a PbsSnapshot> {
    snaps
        .iter()
        .filter(|s| s.backup_id == backup_id)
        .max_by_key(|s| s.backup_time)
}

fn pick_by_time<'a>(snaps: &'a [PbsSnapshot], backup_id: &str, ts: u64) -> Option<&'a PbsSnapshot> {
    snaps
        .iter()
        .filter(|s| s.backup_id == backup_id && s.backup_time <= ts)
        .max_by_key(|s| s.backup_time)
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

fn select_archives_exact_from(
    available: &[String],
    requested: &[String],
    all: bool,
) -> Result<Vec<String>> {
    if all {
        return Ok(available.to_vec());
    }
    if requested.is_empty() {
        return Ok(vec![]);
    }

    let available_set: HashSet<&str> = available.iter().map(|s| s.as_str()).collect();

    let mut out = Vec::with_capacity(requested.len());
    let mut seen = HashSet::<&str>::new();

    for r in requested {
        let r_str = r.as_str();
        if !available_set.contains(r_str) {
            bail!("archive not available from providers: {r}");
        }
        if seen.insert(r_str) {
            out.push(r.clone());
        }
    }

    Ok(out)
}
