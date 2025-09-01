use anyhow::{Result, anyhow};
use std::{
    path::Path,
    time::{Duration, Instant},
};
use tracing as log;

use crate::utils::process::{CmdSpec, Pipeline, Runner, StdioSpec};

pub fn wait_for_block<R: Runner>(dev: &Path, runner: &R) -> Result<()> {
    wait_for_block_with(
        dev,
        Duration::from_secs(5),
        Duration::from_millis(100),
        runner,
    )
}

pub fn wait_for_block_with<R: Runner>(
    dev: &Path,
    timeout: Duration,
    delay: Duration,
    runner: &R,
) -> Result<()> {
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

        let _ = runner.run(
            &Pipeline::new().cmd(
                CmdSpec::new("udevadm")
                    .args(["trigger", "--subsystem-match=block", "--action=add"])
                    .stdout(StdioSpec::Null)
                    .stderr(StdioSpec::Null),
            ),
        );
        let _ = runner.run(
            &Pipeline::new().cmd(
                CmdSpec::new("udevadm")
                    .arg("settle")
                    .stdout(StdioSpec::Null)
                    .stderr(StdioSpec::Null),
            ),
        );

        std::thread::sleep(delay);
    }

    Err(anyhow!("device node did not appear: {}", dev.display()))
}
