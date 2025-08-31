use anyhow::{Context, Result, anyhow, bail};
use config as cfg;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
};

#[derive(Debug, Clone)]
pub struct Config {
    pub pbs: Pbs,
    pub zfs: Option<Zfs>,
    pub lvmthin: Option<LvmThin>,
}

#[derive(Debug, Clone)]
pub struct Pbs {
    pub repos: HashMap<String, String>,
    pub default_repo: Option<String>,
    pub keyfile: Option<PathBuf>,
    pub password: Option<String>,
    pub ns: Option<String>,
    pub backup_id: String,
    pub pv_prefixes: Vec<String>,
    pub pv_exclude_re: Option<Regex>,
    pub pv_exclude_re_src: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Zfs {
    pub pools: Vec<String>,
    pub restore: Option<ZfsRestore>,
}

#[derive(Debug, Clone)]
pub struct ZfsRestore {
    pub dest_root: String,
}

#[derive(Debug, Clone)]
pub struct LvmThin {
    pub vgs: Vec<String>,
    pub restore: Option<LvmThinRestore>,
}

#[derive(Debug, Clone)]
pub struct LvmThinRestore {
    pub vg: String,
    pub thinpool: Option<String>,
}

impl Pbs {
    pub fn repo<'a>(&'a self, sel: Option<&str>) -> Result<&'a str> {
        if let Some(name) = sel {
            return self.repos.get(name).map(|s| s.as_str()).ok_or_else(|| {
                anyhow!("unknown target '{}'; known: {}", name, self.known_targets())
            });
        }
        if let Some(def) = &self.default_repo {
            return self
                .repos
                .get(def)
                .map(|s| s.as_str())
                .ok_or_else(|| anyhow!("default_repo='{}' not found in [pbs.repos]", def));
        }
        if self.repos.len() == 1 {
            let (_k, v) = self.repos.iter().next().unwrap();
            return Ok(v.as_str());
        }
        Err(anyhow!(
            "no target provided and no default_repo set; specify --target <{}>",
            self.known_targets()
        ))
    }

    fn known_targets(&self) -> String {
        let mut keys: Vec<&str> = self.repos.keys().map(|s| s.as_str()).collect();
        keys.sort_unstable();
        keys.join("|")
    }

    pub fn pv_allows(&self, name: &str) -> bool {
        let pref_ok = if self.pv_prefixes.is_empty() {
            true
        } else {
            self.pv_prefixes.iter().any(|p| name.starts_with(p))
        };
        let not_excluded = self
            .pv_exclude_re
            .as_ref()
            .map(|re| !re.is_match(name))
            .unwrap_or(true);
        pref_ok && not_excluded
    }
}

#[derive(Debug, Deserialize)]
struct RawConfig {
    pbs: RawPbs,
    #[serde(default)]
    zfs: Option<RawZfs>,
    #[serde(default)]
    lvmthin: Option<RawLvmThin>,
}

#[derive(Debug, Deserialize)]
struct RawPbs {
    #[serde(default)]
    repos: HashMap<String, String>,
    default_repo: Option<String>,

    keyfile: Option<String>,
    password_file: Option<String>,
    ns: Option<String>,
    backup_id: Option<String>,
    pv_prefixes: Option<Vec<String>>,
    pv_exclude_re: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawZfs {
    pools: Vec<String>,
    #[serde(default)]
    restore: Option<RawZfsRestore>,
}

#[derive(Debug, Deserialize)]
struct RawZfsRestore {
    dest_root: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawLvmThin {
    vgs: Vec<String>,
    #[serde(default)]
    restore: Option<RawLvmThinRestore>,
}

#[derive(Debug, Deserialize)]
struct RawLvmThinRestore {
    vg: Option<String>,
    thinpool: Option<String>,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let base_dir = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));

        let raw: RawConfig = cfg::Config::builder()
            .add_source(cfg::File::from(path))
            .build()
            .with_context(|| format!("load {}", path.display()))?
            .try_deserialize()
            .with_context(|| format!("deserialize {}", path.display()))?;

        let n = config_helpers::Normalizer { base_dir };
        let (repos, default_repo) = Self::build_repos(raw.pbs.repos, raw.pbs.default_repo)?;

        let keyfile = n.trim_opt(raw.pbs.keyfile).map(|s| n.resolve(&s));
        let password = match n.trim_opt(raw.pbs.password_file).map(|s| n.resolve(&s)) {
            Some(p) => Some(
                n.read_secret(&p)
                    .with_context(|| format!("read PBS token from {}", p.display()))?,
            ),
            None => None,
        };
        let ns = n.trim_opt(raw.pbs.ns);
        let backup_id = n
            .trim_opt(raw.pbs.backup_id)
            .unwrap_or_else(|| format!("{}-k8s-pv", n.hostname()));
        let pv_prefixes = raw
            .pbs
            .pv_prefixes
            .unwrap_or_default()
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();

        let pv_exclude_re_src = n.trim_opt(raw.pbs.pv_exclude_re);
        let pv_exclude_re = match &pv_exclude_re_src {
            Some(s) => Some(Regex::new(s).with_context(|| format!("bad pbs.pv_exclude_re: {s}"))?),
            None => None,
        };
        let pbs = Pbs {
            repos,
            default_repo,
            keyfile,
            password,
            ns,
            backup_id,
            pv_prefixes,
            pv_exclude_re,
            pv_exclude_re_src,
        };

        let zfs: Option<Zfs> = match raw.zfs {
            None => None,
            Some(rz) => {
                let pools = n.dedup(rz.pools);
                if pools.is_empty() {
                    bail!("zfs.pools must not be empty");
                }
                let restore = match rz.restore {
                    None => None,
                    Some(rr) => {
                        let dest_root = n.trim_opt(rr.dest_root).unwrap_or_default();
                        if dest_root.is_empty() {
                            bail!("[zfs.restore] dest_root must not be empty");
                        }
                        Some(ZfsRestore { dest_root })
                    }
                };
                Some(Zfs { pools, restore })
            }
        };
        let lvmthin: Option<LvmThin> = match raw.lvmthin {
            None => None,
            Some(rl) => {
                let vgs = n.dedup(rl.vgs);
                if vgs.is_empty() {
                    bail!("lvmthin.vgs must not be empty");
                }
                let restore = match rl.restore {
                    None => None,
                    Some(r) => {
                        let vg = n.trim_opt(r.vg).unwrap_or_default();
                        if vg.is_empty() {
                            bail!("[lvmthin.restore] vg must not be empty");
                        }
                        let thinpool = n.trim_opt(r.thinpool);
                        Some(LvmThinRestore { vg, thinpool })
                    }
                };
                Some(LvmThin { vgs, restore })
            }
        };
        Ok(Self { pbs, zfs, lvmthin })
    }

    fn build_repos(
        raw_repos: HashMap<String, String>,
        default_repo_raw: Option<String>,
    ) -> Result<(HashMap<String, String>, Option<String>)> {
        if raw_repos.is_empty() {
            bail!("define at least one repository under [pbs.repos]");
        }

        let mut repos: HashMap<String, String> = HashMap::with_capacity(raw_repos.len());

        for (raw_name, raw_url) in raw_repos {
            let name = raw_name.trim().to_string();
            if name.is_empty() {
                bail!("empty repo name in [pbs.repos]");
            }
            if !Self::valid_target_name(&name) {
                bail!("bad repo name '{}': use [A-Za-z0-9_-], length 1..32", name);
            }

            let url = raw_url.trim().to_string();
            if url.is_empty() {
                bail!("empty URL for repo '{}'", name);
            }

            if repos.insert(name.clone(), url).is_some() {
                bail!("duplicate repo entry '{}'", name);
            }
        }

        let default_repo = default_repo_raw
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        if let Some(ref def) = default_repo {
            if !repos.contains_key(def) {
                bail!("default_repo='{}' not found in [pbs.repos]", def);
            }
        }

        Ok((repos, default_repo))
    }

    #[inline]
    fn valid_target_name(name: &str) -> bool {
        let len_ok = (1..=32).contains(&name.len());
        len_ok
            && name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    }

    pub fn to_redacted_toml(&self) -> Result<String> {
        #[derive(Serialize)]
        struct PbsOut<'a> {
            repos: BTreeMap<&'a str, &'a str>,
            default_repo: Option<&'a str>,
            keyfile: Option<String>,
            password: &'static str,
            ns: Option<&'a str>,
            backup_id: &'a str,
            pv_prefixes: &'a [String],
            pv_exclude_re: Option<&'a str>,
        }
        #[derive(Serialize)]
        struct ZfsRestoreOut<'a> {
            dest_root: &'a str,
        }
        #[derive(Serialize)]
        struct ZfsOut<'a> {
            pools: &'a [String],
            #[serde(skip_serializing_if = "Option::is_none")]
            restore: Option<ZfsRestoreOut<'a>>,
        }
        #[derive(Serialize)]
        struct LvmThinRestoreOut<'a> {
            vg: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            thinpool: Option<&'a str>,
        }
        #[derive(Serialize)]
        struct LvmThinOut<'a> {
            vgs: &'a [String],
            #[serde(skip_serializing_if = "Option::is_none")]
            restore: Option<LvmThinRestoreOut<'a>>,
        }

        #[derive(Serialize)]
        struct Out<'a> {
            pbs: PbsOut<'a>,
            #[serde(skip_serializing_if = "Option::is_none")]
            zfs: Option<ZfsOut<'a>>,
            #[serde(skip_serializing_if = "Option::is_none")]
            lvmthin: Option<LvmThinOut<'a>>,
        }

        let repos_sorted: BTreeMap<&str, &str> = self
            .pbs
            .repos
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let zfs_out = self.zfs.as_ref().map(|z| ZfsOut {
            pools: &z.pools,
            restore: z.restore.as_ref().map(|r| ZfsRestoreOut {
                dest_root: r.dest_root.as_str(),
            }),
        });

        let lvmthin_out = self.lvmthin.as_ref().map(|l| LvmThinOut {
            vgs: &l.vgs,
            restore: l.restore.as_ref().map(|r| LvmThinRestoreOut {
                vg: r.vg.as_str(),
                thinpool: r.thinpool.as_deref(),
            }),
        });

        let out = Out {
            pbs: PbsOut {
                repos: repos_sorted,
                default_repo: self.pbs.default_repo.as_deref(),
                keyfile: self.pbs.keyfile.as_ref().map(|p| p.display().to_string()),
                password: if self.pbs.password.is_some() {
                    "<redacted>"
                } else {
                    "<none>"
                },
                ns: self.pbs.ns.as_deref(),
                backup_id: &self.pbs.backup_id,
                pv_prefixes: &self.pbs.pv_prefixes,
                pv_exclude_re: self.pbs.pv_exclude_re_src.as_deref(),
            },
            zfs: zfs_out,
            lvmthin: lvmthin_out,
        };
        Ok(toml::to_string_pretty(&out)?)
    }
}

mod config_helpers {
    use anyhow::Result;
    use std::{
        collections::HashSet,
        fs,
        path::{Path, PathBuf},
        process::Command,
    };

    pub(super) struct Normalizer<'a> {
        pub base_dir: &'a Path,
    }

    impl<'a> Normalizer<'a> {
        #[inline]
        pub fn trim_opt(&self, s: Option<String>) -> Option<String> {
            s.map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
        }

        #[inline]
        pub fn resolve(&self, p: &str) -> PathBuf {
            let pb = PathBuf::from(p.trim());
            if pb.is_absolute() {
                pb
            } else {
                self.base_dir.join(pb)
            }
        }

        pub fn read_secret(&self, p: &Path) -> Result<String> {
            let mut s = String::from_utf8(fs::read(p)?)?;
            while s.ends_with('\n') || s.ends_with('\r') {
                s.pop();
            }
            Ok(s)
        }

        pub fn hostname(&self) -> String {
            Command::new("hostname")
                .output()
                .ok()
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "host".into())
        }

        pub fn dedup(&self, items: Vec<String>) -> Vec<String> {
            let mut seen = HashSet::new();
            let mut out = Vec::new();
            for s in items
                .into_iter()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
            {
                if seen.insert(s.clone()) {
                    out.push(s);
                }
            }
            out
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn write(path: &Path, s: &str) {
        fs::write(path, s).unwrap();
    }

    #[test]
    fn load_minimal_ok_and_selection() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        let token = dir.join("token");
        let key = dir.join("enc.key");
        write(&token, "supersecret\n");
        write(&key, "dummykey");

        let cfg_path = dir.join("config.toml");
        write(
            &cfg_path,
            r#" 
[pbs]
ns = "pv"
backup_id = "backup-pv"
password_file = "token"
keyfile = "enc.key"
default_repo = "nas"
pv_exclude_re = "test-.*"

[pbs.repos]
nas = "root@pam!pve@192.168.0.24:nas-store"
s3  = "root@pam!pve@192.168.0.25:s3-store"

[zfs]
pools      = ["tank", " tank "]
"#,
        );

        let cfg = Config::load(&cfg_path).expect("load ok");

        assert_eq!(cfg.pbs.password.as_deref(), Some("supersecret"));
        assert_eq!(cfg.pbs.keyfile.as_deref(), Some(key.as_path()));

        assert_eq!(cfg.pbs.ns.as_deref(), Some("pv"));
        assert_eq!(cfg.pbs.backup_id, "backup-pv");
        assert_eq!(
            cfg.pbs.repo(None).unwrap(),
            "root@pam!pve@192.168.0.24:nas-store"
        );
        assert_eq!(
            cfg.pbs.repo(Some("s3")).unwrap(),
            "root@pam!pve@192.168.0.25:s3-store"
        );

        let err = cfg.pbs.repo(Some("offsite")).unwrap_err().to_string();
        assert!(err.contains("unknown target"));

        let re = cfg.pbs.pv_exclude_re.as_ref().unwrap();
        assert!(re.is_match("test-debug"));

        assert_eq!(cfg.zfs.as_ref().unwrap().pools, vec!["tank".to_string()]);
    }

    #[test]
    fn single_repo_no_default_ok() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        let cfg_path = dir.join("config.toml");
        write(
            &cfg_path,
            r#"
[pbs]
backup_id = "backup-pv"

[pbs.repos]
only = "root@pam!pve@192.168.0.24:nas-store"

[zfs]
pools = ["tank"]

[backup]
clone_suffix = "pbs"
"#,
        );

        let cfg = Config::load(&cfg_path).unwrap();
        assert_eq!(
            cfg.pbs.repo(None).unwrap(),
            "root@pam!pve@192.168.0.24:nas-store"
        );
    }

    #[test]
    fn bad_regex_fails() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        let cfg_path = dir.join("config.toml");
        write(
            &cfg_path,
            r#"
[pbs]
backup_id = "backup-pv"
pv_exclude_re = "("

[pbs.repos]
nas = "root@pam!pve@192.168.0.24:nas-store"

[zfs]
pools = ["tank"]
"#,
        );

        let err = Config::load(&cfg_path).unwrap_err();
        let msg = format!("{err:#}");

        assert!(msg.contains("bad pbs.pv_exclude_re"));
    }

    #[test]
    fn pools_become_empty_after_trim_dedup() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        let cfg_path = dir.join("config.toml");
        write(
            &cfg_path,
            r#"
[pbs]
backup_id = "backup-pv"
[pbs.repos]
nas = "root@pam!pve@192.168.0.24:nas-store"

[zfs]
pools = ["   ", "  "]

[backup]
clone_suffix = "pbs"
"#,
        );

        let err = Config::load(&cfg_path).unwrap_err().to_string();
        assert!(err.contains("zfs.pools became empty"));
    }

    #[test]
    fn print_config_redacts_password_and_sorts_repos() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        write(&dir.join("token"), "sekret");
        write(&dir.join("enc.key"), "k");

        let cfg_path = dir.join("config.toml");
        write(
            &cfg_path,
            r#"
[pbs]
backup_id = "backup-pv"
password_file = "token"
keyfile = "enc.key"
default_repo = "b"

[pbs.repos]
b = "url-b"
a = "url-a"

[zfs]
pools = ["tank"]

[backup]
clone_suffix = "pbs"
"#,
        );

        let cfg = Config::load(&cfg_path).unwrap();
        let printed = cfg.to_redacted_toml().unwrap();

        assert!(printed.contains("password = \"<redacted>\""));
        assert!(!printed.contains("sekret"));

        let pos_a = printed.find("\na = \"url-a\"").unwrap();
        let pos_b = printed.find("\nb = \"url-b\"").unwrap();
        assert!(pos_a < pos_b);

        assert!(printed.contains("default_repo = \"b\""));
    }
}
