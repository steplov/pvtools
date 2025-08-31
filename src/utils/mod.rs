pub mod bins;
pub mod lock;
pub mod process;

pub mod time {
    #[inline]
    pub fn current_epoch() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    #[cfg(test)]
    mod tests {
        #[test]
        fn epoch_nonzero() {
            assert!(super::current_epoch() > 1_600_000_000);
        }
    }
}

pub mod naming {
    use anyhow::{Result, anyhow, bail};
    use std::path::Path;

    pub fn create_archive_name(provider: &str, leaf: &str, id: &str) -> Result<String> {
        let path = Path::new(leaf);

        let stem = path
            .file_stem()
            .ok_or_else(|| anyhow!("invalid leaf, no stem: {leaf}"))?
            .to_string_lossy();

        let ext = path
            .extension()
            .ok_or_else(|| anyhow!("invalid leaf, no extension: {leaf}"))?
            .to_string_lossy();

        Ok(format!("{provider}_{stem}_{ext}_{id}.img"))
    }

    pub fn parse_archive_name(name: &str) -> Result<(String, String, String)> {
        let mut base = name;
        if base.ends_with(".fidx") {
            base = &base[..base.len() - 5];
        }
        if base.ends_with(".img") {
            base = &base[..base.len() - 4];
        }

        let parts: Vec<&str> = base.split('_').collect();
        if parts.len() < 4 {
            bail!("invalid archive name: {name}");
        }

        let provider = parts[0].to_string();
        let id = parts.last().unwrap().to_string();
        let ext = parts[parts.len() - 2];
        let stem = parts[1..parts.len() - 2].join("_");

        let leaf = format!("{stem}.{ext}");
        Ok((provider, leaf, id))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn roundtrip_zfs_raw() {
            let archive = create_archive_name("zfs", "vm-9999-pv-test.raw", "85a081ee").unwrap();
            assert_eq!(archive, "zfs_vm-9999-pv-test_raw_85a081ee.img");

            let (prov, leaf, id) = parse_archive_name(&archive).unwrap();
            assert_eq!(prov, "zfs");
            assert_eq!(leaf, "vm-9999-pv-test.raw");
            assert_eq!(id, "85a081ee");
        }

        #[test]
        fn roundtrip_lvmthin_raw() {
            let archive =
                create_archive_name("lvmthin", "vm-9999-pv-radarr-config.raw", "efae231b").unwrap();
            assert_eq!(archive, "lvmthin_vm-9999-pv-radarr-config_raw_efae231b.img");

            let (prov, leaf, id) = parse_archive_name(&archive).unwrap();
            assert_eq!(prov, "lvmthin");
            assert_eq!(leaf, "vm-9999-pv-radarr-config.raw");
            assert_eq!(id, "efae231b");
        }

        #[test]
        fn roundtrip_qcow2() {
            let archive = create_archive_name("zfs", "vm-1000-data.qcow2", "cafebabe").unwrap();
            assert_eq!(archive, "zfs_vm-1000-data_qcow2_cafebabe.img");

            let (prov, leaf, id) = parse_archive_name(&archive).unwrap();
            assert_eq!(prov, "zfs");
            assert_eq!(leaf, "vm-1000-data.qcow2");
            assert_eq!(id, "cafebabe");
        }

        #[test]
        fn parse_fidx() {
            let archive = "zfs_vm-1000-data_raw_12345678.img.fidx";
            let (prov, leaf, id) = parse_archive_name(archive).unwrap();
            assert_eq!(prov, "zfs");
            assert_eq!(leaf, "vm-1000-data.raw");
            assert_eq!(id, "12345678");
        }
    }
}

pub mod path {
    #[inline]
    pub fn dataset_leaf(s: &str) -> &str {
        s.rsplit('/').next().unwrap_or(s)
    }

    #[cfg(test)]
    mod tests {
        #[test]
        fn leaf_ok() {
            assert_eq!(super::dataset_leaf("a/b/c"), "c");
        }
        #[test]
        fn leaf_root() {
            assert_eq!(super::dataset_leaf("c"), "c");
        }
    }
}

pub mod dev {
    use anyhow::{Result, anyhow};
    use std::{
        path::Path,
        process::Command,
        time::{Duration, Instant},
    };
    use tracing as log;

    pub fn wait_for_block(dev: &Path) -> Result<()> {
        wait_for_block_with(dev, Duration::from_secs(5), Duration::from_millis(100))
    }

    pub fn wait_for_block_with(dev: &Path, timeout: Duration, delay: Duration) -> Result<()> {
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
            let _ = Command::new("udevadm")
                .args(["trigger", "--subsystem-match=block", "--action=add"])
                .status();
            let _ = Command::new("udevadm").arg("settle").status();
            std::thread::sleep(delay);
        }

        Err(anyhow!("device node did not appear: {}", dev.display()))
    }
}

pub mod ids {
    use anyhow::{Context, Result, bail};
    use std::collections::HashMap;
    use std::process::Command;

    pub fn zfs_guids(pool: &str) -> Result<HashMap<String, String>> {
        let out = Command::new("zfs")
            .args(["get", "-H", "-o", "name,value", "guid", "-r", pool])
            .output()
            .with_context(|| format!("zfs get guid -r {pool}"))?;

        if !out.status.success() {
            bail!("zfs get guid failed for pool {pool}: {}", out.status);
        }

        let mut map = HashMap::new();
        for line in String::from_utf8_lossy(&out.stdout).lines() {
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

    pub fn lvmthin_short8(vg: &str, lv: &str) -> Result<String> {
        let out = Command::new("lvs")
            .args(["--noheadings", "-o", "lv_uuid", &format!("{vg}/{lv}")])
            .output()
            .with_context(|| format!("lvs lv_uuid for {vg}/{lv}"))?;
        if !out.status.success() {
            bail!("lvs lv_uuid failed: {}", out.status);
        }
        let mut s = String::from_utf8_lossy(&out.stdout).to_lowercase();
        s.retain(|c| c.is_ascii_hexdigit());
        Ok(s.chars().take(8).collect())
    }
}
