use std::collections::{BTreeSet, HashSet};

use anyhow::{Context, Result, bail};
use tracing as log;

use super::providers::ProviderRegistry;
use crate::{
    AppCtx,
    tooling::{PbsSnapshot, dd::DdOpts},
    ui,
    utils::{
        exec_policy::with_dry_run_enabled,
        lock::LockGuard,
        time::{fmt_utc, parse_rfc3339_to_unix},
    },
    volume::{Volume, VolumeSliceExt},
};

#[derive(Debug, Clone)]
pub enum RestorePoint {
    Latest,
    At(u64),
}

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
            dry_run: value.dry_run,
        })
    }
}

pub fn list_snapshots(ctx: &AppCtx, opts: ListSnapshotsOpts) -> Result<()> {
    let repo = ctx.cfg.pbs.repo_source(opts.source.as_deref())?;
    let ns_opt = ctx.cfg.pbs.ns.as_deref();
    let snaps = ctx.tools.pbs().snapshots(repo, ns_opt)?;

    ui::log_pbs_info(
        repo,
        ctx.cfg.pbs.ns.as_deref(),
        &ctx.cfg.pbs.backup_id,
        None,
    );

    let mut filtered: Vec<&PbsSnapshot> = snaps
        .iter()
        .filter(|s| s.backup_id == ctx.cfg.pbs.backup_id)
        .collect();
    filtered.sort_by_key(|s| s.backup_time);

    let rows: Vec<Vec<String>> = filtered
        .into_iter()
        .rev()
        .map(|s| {
            let when = fmt_utc(s.backup_time).unwrap_or_else(|_| s.backup_time.to_string());

            let files_joined = s
                .files
                .iter()
                .map(|f| f.filename.as_str())
                .filter(|&f| f != "index.json.blob")
                .collect::<Vec<_>>()
                .join("\n");

            let files = if files_joined.is_empty() {
                "-".to_string()
            } else {
                files_joined
            };

            vec![when, files]
        })
        .collect();

    ui::log_snapshots(rows);

    Ok(())
}

pub fn list_archives(ctx: &AppCtx, opts: ListArchivesOpts) -> Result<()> {
    let repo = ctx.cfg.pbs.repo_source(opts.source.as_deref())?;
    let ns_opt = ctx.cfg.pbs.ns.as_deref();
    let point = &opts.snapshot;
    let snaps = ctx.tools.pbs().snapshots(repo, ns_opt)?;

    if snaps.is_empty() {
        bail!("no snapshots found in repo {repo}");
    }

    let snap = pick_snapshot(&snaps, &ctx.cfg.pbs.backup_id, point.clone())?;
    let registry = ProviderRegistry::new(ctx, Some(snap));
    let providers = registry.build();

    let rows: Vec<String> = providers
        .iter()
        .flat_map(|p| p.list_archives(snap))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    ui::log_pbs_info(repo, ns_opt, &snap.backup_id, Some(snap.backup_time));
    ui::log_pbs_archives(rows);

    Ok(())
}

pub fn restore_run(ctx: &AppCtx, opts: RunOpts) -> Result<()> {
    let _lock = LockGuard::try_acquire("pvtool-restore")?;

    with_dry_run_enabled(opts.dry_run, || -> Result<()> {
        let repo = ctx.cfg.pbs.repo_source(opts.source.as_deref())?;
        let ns_opt = ctx.cfg.pbs.ns.as_deref();
        let point = &opts.snapshot;
        let snaps = ctx.tools.pbs().snapshots(repo, ns_opt)?;
        if snaps.is_empty() {
            bail!("no snapshots found in repo {repo}");
        }
        let snap = pick_snapshot(&snaps, &ctx.cfg.pbs.backup_id, point.clone())?;

        let registry = ProviderRegistry::new(ctx, Some(snap));
        let mut providers = registry.build();
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

        let mut items: Vec<Volume> = Vec::new();
        for p in providers.iter_mut() {
            if opts.all {
                let mut r = p
                    .collect_restore(None, true)
                    .with_context(|| format!("collect restore plan from provider {}", p.name()))?;
                items.append(&mut r);
            } else {
                for a in &selected_archives {
                    let mut r =
                        p.collect_restore(Some(a.as_str()), opts.all)
                            .with_context(|| {
                                format!("collect restore plan from provider {}", p.name())
                            })?;
                    items.append(&mut r);
                }
            }
        }

        if items.is_empty() {
            log::info!("nothing to restore");
            return Ok(());
        }

        items.ensure_unique_targets()?;

        log::info!("Plan");
        ui::log_pbs_info(repo, ns_opt, &ctx.cfg.pbs.backup_id, Some(snap.backup_time));
        ui::log_archives(&items);
        log::info!("\n");

        let dd_opts = DdOpts::default();

        for i in &items {
            let dd_cmd = ctx.tools.dd().to_file_cmd(&i.device, &dd_opts);
            ctx.tools
                .pbs()
                .restore_to(
                    repo,
                    ns_opt,
                    &snap.backup_id,
                    &i.archive,
                    ctx.cfg.pbs.keyfile.as_deref(),
                    dd_cmd,
                )
                .with_context(|| format!("restore pipeline for {}", i.archive))?;
        }

        log::info!("done");
        Ok(())
    })
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

fn pick_snapshot<'a>(
    snaps: &'a [PbsSnapshot],
    backup_id: &str,
    point: RestorePoint,
) -> Result<&'a PbsSnapshot> {
    let cand = snaps
        .iter()
        .filter(|s| s.backup_id == backup_id)
        .filter(|s| match point {
            RestorePoint::Latest => true,
            RestorePoint::At(ts) => s.backup_time <= ts,
        })
        .max_by_key(|s| s.backup_time);
    let msg = match point {
        RestorePoint::Latest => format!("no snapshots found for backup-id '{backup_id}'"),
        RestorePoint::At(ts) => {
            format!("no matching snapshot found before given time {ts} for backup-id '{backup_id}'")
        }
    };

    cand.with_context(|| msg)
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
