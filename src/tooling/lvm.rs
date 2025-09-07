use std::sync::Arc;

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::utils::process::{CmdSpec, Pipeline, Runner, StdioSpec};

pub const REQ_BINS: &[&str] = &["lvs", "lvcreate", "lvchange", "lvremove"];

#[derive(Deserialize)]
struct LvsJson {
    report: Vec<Report>,
}

#[derive(Deserialize)]
struct Report {
    lv: Vec<LvInfo>,
}

#[derive(Deserialize)]
pub struct LvInfo {
    pub lv_name: String,
    pub vg_name: String,
    #[serde(default)]
    pub segtype: Option<String>,
}

pub trait LvmPort: Send + Sync {
    fn list_lvs(&self) -> Result<Vec<LvInfo>>;
    fn lvcreate_snapshot(&self, vg: &str, lv: &str, snap: &str) -> Result<String>;
    fn lvchange_activate(&self, lv_fq: &str) -> Result<()>;
    fn lvremove_force(&self, lv_fq: &str) -> Result<()>;
    fn lv_name(&self, vg: &str, lv: &str) -> Result<String>;
    fn lv_uuid_short8(&self, vg: &str, lv: &str) -> Result<String>;
    fn lvcreate_thin(
        &self,
        vg: &str,
        thinpool: &str,
        name: &str,
        size_bytes: u64,
    ) -> anyhow::Result<()>;
}

type DynRunner = dyn Runner + Send + Sync;

pub struct LvmCli {
    runner: Arc<DynRunner>,
}

impl LvmCli {
    pub fn new(runner: Arc<DynRunner>) -> Self {
        Self { runner }
    }

    #[inline]
    fn lvs(&self) -> CmdSpec {
        CmdSpec::new("lvs")
    }
    #[inline]
    fn lvcreate(&self) -> CmdSpec {
        CmdSpec::new("lvcreate")
    }
    #[inline]
    fn lvchange(&self) -> CmdSpec {
        CmdSpec::new("lvchange")
    }
    #[inline]
    fn lvremove(&self) -> CmdSpec {
        CmdSpec::new("lvremove")
    }
}

impl LvmPort for LvmCli {
    fn list_lvs(&self) -> Result<Vec<LvInfo>> {
        let cmd = self
            .lvs()
            .args([
                "--reportformat",
                "json",
                "--units",
                "b",
                "-o",
                "lv_name,vg_name,segtype",
            ])
            .stdout(StdioSpec::Pipe)
            .stderr(StdioSpec::Inherit);

        let out = self
            .runner
            .run_capture(&Pipeline::new().cmd(cmd))
            .context("run lvs")?;

        let json: LvsJson = serde_json::from_str(&out).context("parse lvs json")?;
        Ok(json
            .report
            .into_iter()
            .flat_map(|r| r.lv)
            .map(|r| LvInfo {
                lv_name: r.lv_name,
                vg_name: r.vg_name,
                segtype: r.segtype,
            })
            .collect())
    }

    fn lvcreate_snapshot(&self, vg: &str, lv: &str, snap: &str) -> Result<String> {
        let src = format!("{vg}/{lv}");
        let cmd = self
            .lvcreate()
            .args(["-s", "-n", snap, &src])
            .stderr(StdioSpec::Inherit)
            .stdout(StdioSpec::Inherit);

        self.runner
            .run(&Pipeline::new().cmd(cmd))
            .with_context(|| format!("lvcreate -s -n {snap} {src}"))?;

        Ok(format!("{vg}/{snap}"))
    }

    fn lvchange_activate(&self, lv_fq: &str) -> Result<()> {
        let cmd = self
            .lvchange()
            .args(["-K", "-ay", lv_fq])
            .stderr(StdioSpec::Inherit)
            .stdout(StdioSpec::Inherit);

        self.runner
            .run(&Pipeline::new().cmd(cmd))
            .with_context(|| format!("lvchange -K -ay {lv_fq}"))
    }

    fn lvremove_force(&self, lv_fq: &str) -> Result<()> {
        let cmd = self
            .lvremove()
            .args(["-f", lv_fq])
            .stderr(StdioSpec::Inherit)
            .stdout(StdioSpec::Inherit);

        self.runner
            .run(&Pipeline::new().cmd(cmd))
            .with_context(|| format!("lvremove -f {lv_fq}"))
    }

    fn lv_name(&self, vg: &str, lv: &str) -> Result<String> {
        let target = format!("{vg}/{lv}");
        let cmd = self
            .lvs()
            .args(["--noheadings", "-o", "lv_name", &target])
            .stdout(StdioSpec::Null)
            .stderr(StdioSpec::Null);

        let out = self
            .runner
            .run_capture(&Pipeline::new().cmd(cmd))
            .with_context(|| format!("lvs name for {target}"))?;

        Ok(out)
    }

    fn lv_uuid_short8(&self, vg: &str, lv: &str) -> Result<String> {
        let target = format!("{vg}/{lv}");
        let cmd = self
            .lvs()
            .args(["--noheadings", "-o", "lv_uuid", &target])
            .stdout(StdioSpec::Pipe)
            .stderr(StdioSpec::Null);

        let out = self
            .runner
            .run_capture(&Pipeline::new().cmd(cmd))
            .with_context(|| format!("lvs lv_uuid for {target}"))?;

        let token = out
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_lowercase();

        if token.is_empty() {
            anyhow::bail!("empty lv_uuid output");
        }

        let short8: String = token
            .chars()
            .filter(|c| c.is_ascii_hexdigit())
            .take(8)
            .collect();
        if short8.len() == 8 {
            Ok(short8)
        } else {
            anyhow::bail!("unexpected lv_uuid output for {target}: '{out}'");
        }
    }

    fn lvcreate_thin(
        &self,
        vg: &str,
        thinpool: &str,
        name: &str,
        size_bytes: u64,
    ) -> anyhow::Result<()> {
        let src = format!("{vg}/{thinpool}");
        let cmd = self
            .lvcreate()
            .args(["-T", &src, "-n", name, "-V", &format!("{}B", &size_bytes)])
            .stderr(StdioSpec::Inherit)
            .stdout(StdioSpec::Inherit);

        self.runner
            .run(&Pipeline::new().cmd(cmd))
            .with_context(|| format!("lvcreate -T {src} -n {name} -V {size_bytes}B"))?;

        Ok(())
    }
}
