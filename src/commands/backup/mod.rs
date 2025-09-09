use anyhow::Result;
use clap::{Args, Subcommand};

use crate::AppCtx;

mod executor;
mod providers;

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

#[derive(Args, Debug)]
pub struct BackupRunArgs {
    #[arg(long)]
    pub target: Option<String>,

    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Args, Debug)]
pub struct ListArchivesArgs {
    #[arg(long)]
    pub target: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum BackupCmd {
    Run(BackupRunArgs),
    ListArchives(ListArchivesArgs),
}

impl BackupCmd {
    pub fn run(&self, ctx: &AppCtx) -> Result<()> {
        match self {
            BackupCmd::Run(args) => executor::backup(ctx, args.target.as_deref(), args.dry_run),
            BackupCmd::ListArchives(_args) => executor::list_archives(ctx),
        }
    }
}
