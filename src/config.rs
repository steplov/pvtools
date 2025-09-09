use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow, bail};
use config as cfg;
use regex::Regex;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct Config {
    pub pbs: Pbs,
    pub backup: Backup,
    pub restore: Restore,
}

#[derive(Debug, Clone)]
pub struct Pbs {
    pub repos: HashMap<String, String>,
    pub keyfile: Option<PathBuf>,
    pub password: Option<String>,
    pub ns: Option<String>,
    pub backup_id: String,
}
#[derive(Debug, Clone, Default)]
pub struct Backup {
    pub target: BackupTarget,
    pub sources: BackupSources,
    pub pv_prefixes: Vec<String>,
    pub pv_exclude_re: Option<Regex>,
    pub pv_exclude_re_src: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct BackupTarget {
    pub repo: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct BackupSources {
    pub zfs: Option<Zfs>,
    pub lvmthin: Option<LvmThin>,
}

#[derive(Debug, Clone)]
pub struct Zfs {
    pub pools: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct LvmThin {
    pub vgs: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct Restore {
    pub targets: BTreeMap<String, RestoreTarget>,
    pub rules: Vec<RestoreRule>,
    pub default_target: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum RestoreTarget {
    Zfs {
        root: String,
    },
    LvmThin {
        vg: String,
        thinpool: String,
    },
}

impl fmt::Display for RestoreTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RestoreTarget::Zfs { root } => write!(f, "zfs(root={})", root),
            RestoreTarget::LvmThin { vg, thinpool } => {
                write!(f, "lvmthin(vg={}, thinpool={})", vg, thinpool)
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RestoreRule {
    #[serde(rename = "match.provider")]
    pub match_provider: String,
    #[serde(rename = "match.archive_regex")]
    pub match_archive_regex: Option<String>,
    pub target: String,
}

impl Pbs {
    pub fn repo_by_alias<'a>(&'a self, alias: &str) -> Result<&'a str> {
        self.repos.get(alias).map(|s| s.as_str()).ok_or_else(|| {
            anyhow!(
                "unknown repo alias '{}'; known: {}",
                alias,
                Self::join_aliases(&self.repos)
            )
        })
    }

    #[inline]
    fn join_aliases(repos: &HashMap<String, String>) -> String {
        let mut keys: Vec<&str> = repos.keys().map(|s| s.as_str()).collect();
        keys.sort_unstable();
        keys.join("|")
    }
}

impl Backup {
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

impl Config {
    pub fn resolve_backup_repo<'a>(&'a self, sel: Option<&str>) -> Result<&'a str> {
        if let Some(alias) = sel {
            return self.pbs.repo_by_alias(alias);
        }
        if let Some(default_alias) = self.backup.target.repo.as_deref() {
            return self.pbs.repo_by_alias(default_alias);
        }
        bail!(
            "no backup target provided; specify --target <{}> or set [backup.target].repo",
            Pbs::join_aliases(&self.pbs.repos)
        );
    }
    pub fn resolve_source_repo<'a>(&'a self, sel: Option<&str>) -> Result<&'a str> {
        if let Some(alias) = sel {
            return self.pbs.repo_by_alias(alias);
        }
        bail!(
            "no source provided; specify --source <{}>",
            Pbs::join_aliases(&self.pbs.repos)
        );
    }

    pub fn known_repo_aliases(&self) -> String {
        Pbs::join_aliases(&self.pbs.repos)
    }
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
        let repos = Self::build_repos(raw.pbs.repos)?;
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
            .unwrap_or_else(|| format!("{}-backup", n.hostname()));
        let pbs = Pbs {
            repos,
            keyfile,
            password,
            ns,
            backup_id,
        };

        let pv_prefixes = raw
            .backup
            .pv_prefixes
            .unwrap_or_default()
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();
        let pv_exclude_re_src = n.trim_opt(raw.backup.pv_exclude_re);
        let pv_exclude_re = match &pv_exclude_re_src {
            Some(s) => Some(Regex::new(s).with_context(|| format!("bad pbs.pv_exclude_re: {s}"))?),
            None => None,
        };
        let mut sources = BackupSources::default();
        if let Some(bs) = raw.backup.sources {
            if let Some(z) = bs.zfs {
                let pools = n.dedup(z.pools);
                if pools.is_empty() {
                    bail!("backup.sources.zfs.pools must not be empty");
                }
                sources.zfs = Some(Zfs { pools });
            }
            if let Some(l) = bs.lvmthin {
                let vgs = n.dedup(l.vgs);
                if vgs.is_empty() {
                    bail!("backup.sources.lvmthin.vgs must not be empty");
                }
                sources.lvmthin = Some(LvmThin { vgs });
            }
        }
        let backup = Backup {
            target: BackupTarget {
                repo: raw.backup.target.and_then(|t| n.trim_opt(t.repo)),
            },
            sources,
            pv_prefixes,
            pv_exclude_re,
            pv_exclude_re_src,
        };
        let mut targets: BTreeMap<String, RestoreTarget> = BTreeMap::new();
        if let Some(rt) = raw.restore.targets {
            for (name_raw, t) in rt {
                let name = name_raw.trim().to_string();
                if name.is_empty() {
                    bail!("empty restore target name");
                }
                if !Self::valid_name(&name) {
                    bail!(
                        "bad restore target name '{}': use [A-Za-z0-9_-], length 1..32",
                        name
                    );
                }
                let normalized = match t {
                    RawRestoreTarget::Zfs { root } => {
                        let root = n.trim_opt(root).ok_or_else(|| {
                            anyhow!("[restore.targets.{name}] root must not be empty")
                        })?;
                        RestoreTarget::Zfs { root }
                    }
                    RawRestoreTarget::LvmThin { vg, thinpool } => {
                        let vg = n.trim_opt(vg).ok_or_else(|| {
                            anyhow!("[restore.targets.{name}] vg must not be empty")
                        })?;
                        let thinpool = n.trim_opt(thinpool).ok_or_else(|| {
                            anyhow!("[restore.targets.{name}] thinpool must not be empty")
                        })?;
                        RestoreTarget::LvmThin { vg, thinpool }
                    }
                };
                if targets.insert(name.clone(), normalized).is_some() {
                    bail!("duplicate restore target '{}'", name);
                }
            }
        }
        let mut rules: Vec<RestoreRule> = Vec::new();
        if let Some(rr) = raw.restore.rules {
            let mut seen = BTreeSet::<(String, String)>::new();
            for r in rr {
                let provider = r.match_provider.trim().to_string();
                if provider.is_empty() {
                    bail!("[restore.rules] match.provider must not be empty");
                }
                if !matches!(provider.as_str(), "zfs" | "lvmthin") {
                    bail!("[restore.rules] unknown provider '{}'", provider);
                }
                let target = r.target.trim().to_string();
                if target.is_empty() {
                    bail!("[restore.rules] target must not be empty");
                }

                if let Some(re_src) = &r.match_archive_regex {
                    Regex::new(re_src).with_context(|| {
                        format!("[restore.rules] bad match.archive_regex '{}'", re_src)
                    })?;
                }

                let match_archive_regex = match r
                    .match_archive_regex
                    .as_ref()
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                {
                    Some(src) => {
                        let _compiled = Regex::new(src).with_context(|| {
                            format!("[restore.rules] bad match.archive_regex '{}'", src)
                        })?;
                        Some(src.to_string())
                    }
                    None => None,
                };

                if !seen.insert((provider.clone(), target.clone())) {
                    bail!(
                        "[restore.rules] duplicate rule for provider='{}' target='{}'",
                        provider,
                        target
                    );
                }

                rules.push(RestoreRule {
                    match_provider: provider,
                    match_archive_regex,
                    target,
                });
            }
        }
        let restore = Restore {
            targets,
            rules,
            default_target: n.trim_opt(raw.restore.default_target),
        };
        Ok(Self {
            pbs,
            backup,
            restore,
        })
    }

    fn build_repos(raw_repos: HashMap<String, String>) -> Result<HashMap<String, String>> {
        if raw_repos.is_empty() {
            bail!("define at least one repository under [pbs.repos]");
        }

        let mut repos: HashMap<String, String> = HashMap::with_capacity(raw_repos.len());

        for (raw_name, raw_url) in raw_repos {
            let name = raw_name.trim().to_string();
            if name.is_empty() {
                bail!("empty repo name in [pbs.repos]");
            }
            if !Self::valid_name(&name) {
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
        Ok(repos)
    }

    #[inline]
    fn valid_name(name: &str) -> bool {
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
            keyfile: Option<String>,
            password: &'static str,
            ns: Option<&'a str>,
            backup_id: &'a str,
        }
        #[derive(Serialize, Default)]
        struct BackupSourcesOut<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            zfs: Option<ZfsOut<'a>>,
            #[serde(skip_serializing_if = "Option::is_none")]
            lvmthin: Option<LvmThinOut<'a>>,
        }
        #[derive(Serialize)]
        struct BackupOut<'a> {
            target: BackupTargetOut<'a>,
            #[serde(skip_serializing_if = "is_empty_sources")]
            sources: BackupSourcesOut<'a>,
            pv_prefixes: &'a [String],
            pv_exclude_re: Option<&'a str>,
        }
        #[derive(Serialize)]
        struct BackupTargetOut<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            repo: Option<&'a str>,
        }
        #[derive(Serialize)]
        struct ZfsOut<'a> {
            pools: &'a [String],
        }
        #[derive(Serialize)]
        struct LvmThinOut<'a> {
            vgs: &'a [String],
        }
        #[derive(Serialize)]
        struct RestoreOut<'a> {
            #[serde(skip_serializing_if = "BTreeMap::is_empty")]
            targets: BTreeMap<&'a str, &'a RestoreTarget>,
            #[serde(skip_serializing_if = "is_empty_slice")]
            rules: &'a [RestoreRule],
            #[serde(skip_serializing_if = "Option::is_none")]
            default_target: Option<&'a str>,
        }
        #[derive(Serialize)]
        struct Out<'a> {
            pbs: PbsOut<'a>,
            backup: BackupOut<'a>,
            restore: RestoreOut<'a>,
        }
        fn is_empty_sources(s: &BackupSourcesOut<'_>) -> bool {
            s.zfs.is_none() && s.lvmthin.is_none()
        }

        let repos_sorted: BTreeMap<&str, &str> = self
            .pbs
            .repos
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let sources_out = BackupSourcesOut {
            zfs: self
                .backup
                .sources
                .zfs
                .as_ref()
                .map(|z| ZfsOut { pools: &z.pools }),
            lvmthin: self
                .backup
                .sources
                .lvmthin
                .as_ref()
                .map(|l| LvmThinOut { vgs: &l.vgs }),
        };

        let restore_targets_sorted: BTreeMap<&str, &RestoreTarget> = self
            .restore
            .targets
            .iter()
            .map(|(k, v)| (k.as_str(), v))
            .collect();

        let out = Out {
            pbs: PbsOut {
                repos: repos_sorted,
                keyfile: self.pbs.keyfile.as_ref().map(|p| p.display().to_string()),
                password: if self.pbs.password.is_some() {
                    "<redacted>"
                } else {
                    "<none>"
                },
                ns: self.pbs.ns.as_deref(),
                backup_id: &self.pbs.backup_id,
            },
            backup: BackupOut {
                target: BackupTargetOut {
                    repo: self.backup.target.repo.as_deref(),
                },
                sources: sources_out,
                pv_prefixes: &self.backup.pv_prefixes,
                pv_exclude_re: self.backup.pv_exclude_re_src.as_deref(),
            },
            restore: RestoreOut {
                targets: restore_targets_sorted,
                rules: &self.restore.rules,
                default_target: self.restore.default_target.as_deref(),
            },
        };
        Ok(toml::to_string_pretty(&out)?)
    }
}

#[derive(Debug, Deserialize)]
struct RawConfig {
    pbs: RawPbs,

    #[serde(default)]
    backup: RawBackup,

    #[serde(default)]
    restore: RawRestore,
}

#[derive(Debug, Deserialize)]
struct RawPbs {
    #[serde(default)]
    repos: HashMap<String, String>,
    keyfile: Option<String>,
    password_file: Option<String>,
    ns: Option<String>,
    backup_id: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct RawBackup {
    #[serde(default)]
    target: Option<RawBackupTarget>,
    #[serde(default)]
    sources: Option<RawBackupSources>,
    pv_prefixes: Option<Vec<String>>,
    pv_exclude_re: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawBackupTarget {
    repo: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct RawBackupSources {
    #[serde(default)]
    zfs: Option<RawZfs>,
    #[serde(default)]
    lvmthin: Option<RawLvmThin>,
}
#[derive(Debug, Deserialize)]
struct RawZfs {
    pools: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RawLvmThin {
    vgs: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct RawRestore {
    #[serde(default)]
    targets: Option<BTreeMap<String, RawRestoreTarget>>,
    #[serde(default)]
    rules: Option<Vec<RestoreRule>>,
    #[serde(default)]
    default_target: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
enum RawRestoreTarget {
    #[serde(rename = "zfs")]
    Zfs { root: Option<String> },

    #[serde(rename = "lvmthin")]
    LvmThin {
        vg: Option<String>,
        thinpool: Option<String>,
    },
}

fn is_empty_slice<T>(s: &&[T]) -> bool {
    s.is_empty()
}

mod config_helpers {
    use std::{
        collections::HashSet,
        fs,
        path::{Path, PathBuf},
        process::Command,
    };

    use anyhow::Result;

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
    use std::fs;

    use tempfile::TempDir;

    use super::*;

    fn write(path: &Path, s: &str) {
        fs::write(path, s).unwrap();
    }

    #[test]
    fn load_minimal_ok_and_selection_new_layout() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        write(&dir.join("token"), "sekret");

        let cfg_path = dir.join("config.toml");
        write(
            &cfg_path,
            r#"
[pbs]
backup_id = "backup-pv"
password_file = "token"
[pbs.repos]
a = "url-a"
b = "url-b"

[backup]

[backup.target]
repo = "b"

[backup.sources.zfs]
pools = ["tank"]

[restore.targets.z]
type = "zfs"
root = "tank"

[[restore.rules]]
"match.provider" = "zfs"
target = "z"
"#,
        );

        let cfg = Config::load(&cfg_path).unwrap();
        assert_eq!(cfg.resolve_backup_repo(None).unwrap(), "url-b");
        assert_eq!(cfg.backup.sources.zfs.as_ref().unwrap().pools, vec!["tank"]);
        assert!(cfg.restore.targets.contains_key("z"));
        assert_eq!(cfg.pbs.password.as_deref(), Some("sekret"));
    }

    #[test]
    fn print_config_redacts_and_sorts() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        write(&dir.join("token"), "sekret");

        let cfg_path = dir.join("config.toml");
        write(
            &cfg_path,
            r#"
[pbs]
backup_id = "id"
password_file = "token"
[pbs.repos]
b = "url-b"
a = "url-a"

[backup]

[backup.target]
repo = "a"

[backup.sources.lvmthin]
vgs = ["pve"]

[restore.targets.l]
type = "lvmthin"
vg = "pve"
thinpool = "data"

[[restore.rules]]
"match.provider" = "lvmthin"
target = "l"
"#,
        );

        let cfg = Config::load(&cfg_path).unwrap();
        let printed = cfg.to_redacted_toml().unwrap();
        assert!(printed.contains(r#"password = "<redacted>""#));
        assert!(
            printed.find("\na = \"url-a\"").unwrap() < printed.find("\nb = \"url-b\"").unwrap()
        );
        assert!(printed.contains("[backup.target]"));
        assert!(printed.contains("[restore.targets.l]"));
    }
}
