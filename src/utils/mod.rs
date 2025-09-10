pub mod bins;
pub mod exec_policy;
pub mod lock;
pub mod process;

pub mod time {
    use anyhow::{Context, Result, anyhow};
    use time::{OffsetDateTime, UtcOffset, format_description::well_known::Rfc3339};

    #[inline]
    pub fn current_epoch() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    pub fn fmt_utc(ts: u64) -> Result<String> {
        let ts = i64::try_from(ts).map_err(|_| anyhow!("unix timestamp doesn't fit into i64"))?;
        let dt = OffsetDateTime::from_unix_timestamp(ts)?;
        Ok(dt.format(&Rfc3339)?)
    }

    pub fn parse_rfc3339_to_unix(s: &str) -> Result<u64> {
        let dt = OffsetDateTime::parse(s, &Rfc3339)
            .with_context(|| format!("invalid RFC3339 datetime: {s}"))?
            .to_offset(UtcOffset::UTC);

        let ts = dt.unix_timestamp();
        u64::try_from(ts).map_err(|_| anyhow!("timestamp is negative: {}", ts))
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
    use std::path::Path;

    use anyhow::{Result, anyhow, bail};

    const NO_EXT_SENTINEL: &str = "noext";
    pub fn create_archive_name(provider: &str, leaf: &str, id: &str) -> Result<String> {
        let path = Path::new(leaf);

        let stem = path
            .file_stem()
            .ok_or_else(|| anyhow!("invalid leaf, no stem: {leaf}"))?
            .to_string_lossy();

        let ext = path
            .extension()
            .map(|e| e.to_string_lossy().into_owned())
            .unwrap_or_else(|| NO_EXT_SENTINEL.to_string());

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

        let leaf = if ext == NO_EXT_SENTINEL {
            stem
        } else {
            format!("{stem}.{ext}")
        };

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
        #[test]
        fn roundtrip_no_extension() {
            let archive = create_archive_name("zfs", "vm-42", "deadbeef").unwrap();
            assert_eq!(archive, "zfs_vm-42_noext_deadbeef.img");

            let (prov, leaf, id) = parse_archive_name(&archive).unwrap();
            assert_eq!(prov, "zfs");
            assert_eq!(leaf, "vm-42");
            assert_eq!(id, "deadbeef");
        }

        #[test]
        fn roundtrip_with_underscores_in_leaf() {
            let archive = create_archive_name("zfs", "vm_100-backup.v1.raw", "abcd1234").unwrap();
            assert_eq!(archive, "zfs_vm_100-backup.v1_raw_abcd1234.img");

            let (prov, leaf, id) = parse_archive_name(&archive).unwrap();
            assert_eq!(prov, "zfs");
            assert_eq!(leaf, "vm_100-backup.v1.raw");
            assert_eq!(id, "abcd1234");
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
