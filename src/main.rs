use anyhow::Result;
use clap::{CommandFactory, Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::{EnvFilter, fmt};

mod commands;
mod config;
mod utils;

use commands::backup;
use commands::restore;
use config::Config;

pub struct AppCtx {
    pub debug: bool,
    pub cfg: Config,
}

#[derive(Parser, Debug)]
#[command(
    name = "pvtools",
    about = "Kubernetes PV backup/restore helper for ZFS + Proxmox Backup Server",
    arg_required_else_help = false,
    version = env!("CARGO_PKG_VERSION")
)]
struct Cli {
    #[arg(long, default_value = "./config.toml", global = true)]
    config: PathBuf,

    #[arg(long, global = true)]
    debug: bool,

    #[arg(long, global = true)]
    check_config: bool,

    #[arg(long, global = true)]
    print_config: bool,

    #[command(subcommand)]
    command: Option<Cmd>,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    Backup(backup::BackupArgs),
    Restore(restore::RestoreArgs),
}

fn init_tracing(debug: bool) {
    let default = if debug { "trace" } else { "info" };
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default));
    let _ = fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_file(debug)
        .with_line_number(debug)
        .without_time()
        .try_init();
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.debug);

    if cli.command.is_none() && !cli.check_config && !cli.print_config {
        let mut cmd = Cli::command();
        cmd.print_help()?;
        println!();
        return Ok(());
    }
    let cfg = Config::load(&cli.config)?;

    if cli.check_config {
        tracing::info!("config OK");
        return Ok(());
    }
    if cli.print_config {
        println!("{}", cfg.to_redacted_toml()?);
        return Ok(());
    }

    let Some(cmd) = cli.command else {
        let mut cmd = Cli::command();
        cmd.print_help()?;
        println!();
        return Ok(());
    };

    let ctx = AppCtx {
        debug: cli.debug,
        cfg,
    };

    match cmd {
        Cmd::Backup(args) => args.run(&ctx),
        Cmd::Restore(args) => args.run(&ctx),
    }
}
