use anyhow::{Context, Result};
use std::collections::HashMap;

use crate::utils::process::{CmdSpec, Pipeline, Runner, StdioSpec};

pub fn zfs_guids(pool: &str, runner: &dyn Runner) -> Result<HashMap<String, String>> {
    let cmd = CmdSpec::new("zfs")
        .args(["get", "-H", "-o", "name,value", "guid", "-r", pool])
        .stdout(StdioSpec::Pipe)
        .stderr(StdioSpec::Null);

    let out = runner
        .run_capture(&Pipeline::new().cmd(cmd))
        .with_context(|| format!("zfs get guid -r {pool}"))?;

    let mut map = HashMap::new();
    for line in out.lines() {
        let mut it = line.split_whitespace();
        if let (Some(ds), Some(guid_str)) = (it.next(), it.next()) {
            let n: u128 = guid_str.trim().parse().unwrap_or(0);
            let hex = format!("{n:x}");
            let short = hex.chars().take(8).collect::<String>();
            map.insert(ds.to_string(), short);
        }
    }
    Ok(map)
}

pub fn lvmthin_short8(vg: &str, lv: &str, runner: &dyn Runner) -> Result<String> {
    let cmd = CmdSpec::new("lvs")
        .args(["--noheadings", "-o", "lv_uuid", &format!("{vg}/{lv}")])
        .stdout(StdioSpec::Pipe)
        .stderr(StdioSpec::Null);

    let out = runner
        .run_capture(&Pipeline::new().cmd(cmd))
        .with_context(|| format!("lvs lv_uuid for {vg}/{lv}"))?;

    let mut s = out.to_lowercase();
    s.retain(|c| c.is_ascii_hexdigit());
    Ok(s.chars().take(8).collect())
}
