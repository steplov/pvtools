use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow};
use tracing as log;

use crate::utils::{
    exec_policy,
    process::{CmdSpec, Pipeline, Runner, StdioSpec},
};

pub const REQ_BINS: &[&str] = &["udevadm"];

pub trait BlockPort: Send + Sync {
    fn wait_for_block(&self, dev: &Path) -> Result<()>;
    fn wait_for_block_with(&self, dev: &Path, timeout: Duration, delay: Duration) -> Result<()>;
}

type DynRunner = dyn Runner + Send + Sync;

pub struct BlockCli {
    runner: Arc<DynRunner>,
}

impl BlockCli {
    pub fn new(runner: Arc<DynRunner>) -> Self {
        Self { runner }
    }

    #[inline]
    fn udev_trigger_cmd(&self) -> CmdSpec {
        CmdSpec::new("udevadm")
            .args(["trigger", "--subsystem-match=block", "--action=add"])
            .stdout(StdioSpec::Null)
            .stderr(StdioSpec::Null)
    }

    #[inline]
    fn udev_settle_cmd(&self) -> CmdSpec {
        CmdSpec::new("udevadm")
            .arg("settle")
            .stdout(StdioSpec::Null)
            .stderr(StdioSpec::Null)
    }
}

impl BlockPort for BlockCli {
    fn wait_for_block(&self, dev: &Path) -> Result<()> {
        self.wait_for_block_with(dev, Duration::from_secs(5), Duration::from_millis(100))
    }

    fn wait_for_block_with(&self, dev: &Path, timeout: Duration, delay: Duration) -> Result<()> {
        if exec_policy::is_dry_run() {
            log::info!("[wait] DRY-RUN: skip waiting for {}", dev.display());
            return Ok(());
        }

        let start = Instant::now();
        let mut warned = false;

        while start.elapsed() < timeout {
            if dev.exists() {
                return Ok(());
            }
            if start.elapsed() > Duration::from_secs(1) && !warned {
                log::info!("[wait] device {} not ready, waiting…", dev.display());
                warned = true;
            }

            let _ = self
                .runner
                .run(&Pipeline::new().cmd(self.udev_trigger_cmd()));
            let _ = self
                .runner
                .run(&Pipeline::new().cmd(self.udev_settle_cmd()));

            std::thread::sleep(delay);
        }

        Err(anyhow!("device node did not appear: {}", dev.display()))
    }
}
