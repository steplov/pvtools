use anyhow::Result;
use clap::{Args, Subcommand};

use crate::AppCtx;

mod executor;
mod matcher;
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
    #[arg(long)]
    pub source: Option<String>,
    #[arg(long, default_value = "latest")]
    pub snapshot: String,
}

#[derive(Args, Debug, Clone)]
pub struct RestoreRunArgs {
    #[arg(long)]
    pub source: Option<String>,
    #[arg(long, default_value = "latest")]
    pub snapshot: String,
    #[arg(long = "archive")]
    pub archives: Vec<String>,
    #[arg(long)]
    pub all: bool,
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
