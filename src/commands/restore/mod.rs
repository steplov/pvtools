use anyhow::Result;
use clap::{Args, Subcommand};

use crate::AppCtx;

mod executor;
mod providers;

#[derive(Debug, Args)]
pub struct RestoreArgs {
    #[command(subcommand)]
    pub cmd: RestoreCmd,
}

impl RestoreArgs {
    pub fn run(&self, ctx: &AppCtx) -> Result<()> {
        self.cmd.run(ctx)
    }
}

#[derive(Debug, Subcommand)]
pub enum RestoreCmd {
    ListSnapshots(ListSnapshotsArgs),
    ListArchives(ListArchivesArgs),
    Run(RestoreRunArgs),
}

#[derive(Args, Debug, Clone)]
pub struct ListSnapshotsArgs {
    #[arg(long)]
    pub source: Option<String>,
}

#[derive(Args, Debug, Clone)]
pub struct ListArchivesArgs {
    /// Source PBS repo name
    #[arg(long)]
    pub source: Option<String>,
    /// Snapshot to inspect: "latest" (default) or epoch
    #[arg(long, default_value = "latest")]
    pub snapshot: String,
}

#[derive(Args, Debug, Clone)]
pub struct RestoreRunArgs {
    /// Source PBS repo name
    #[arg(long)]
    pub source: Option<String>,
    /// Snapshot to restore from: "latest" (default) or epoch
    #[arg(long, default_value = "latest")]
    pub snapshot: String,
    /// Restore these archives only (can be repeated).
    /// Accepts full PBS filename (e.g. zfs_vm-...img.fidx/img)
    #[arg(long = "archive")]
    pub archives: Vec<String>,
    /// Restore all archives from the snapshot
    #[arg(long)]
    pub all: bool,
    /// Overwrite existing volumes (provider may use this to allow override)
    #[arg(long)]
    pub force: bool,
    /// Show what would be restored without executing
    #[arg(long)]
    pub dry_run: bool,
}

impl RestoreCmd {
    pub fn run(&self, ctx: &AppCtx) -> Result<()> {
        match self {
            RestoreCmd::ListSnapshots(args) => {
                let opts = executor::ListSnapshotsOpts::from(args);
                executor::list_snapshots(ctx, opts)
            }
            RestoreCmd::ListArchives(args) => {
                let opts = executor::ListArchivesOpts::try_from(args)?;
                executor::list_archives(ctx, opts)
            }
            RestoreCmd::Run(args) => {
                let opts = executor::RunOpts::try_from(args)?;
                executor::restore_run(ctx, opts)
            }
        }
    }
}
