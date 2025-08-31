use crate::utils::shell::sh_quote;
use anyhow::{Context, Result, bail};
use std::process::{Command, Stdio};
use std::{env, ffi::OsString};

#[cfg(test)]
static PBS_BIN_OVERRIDE: std::sync::OnceLock<OsString> = std::sync::OnceLock::new();

fn pbs_bin() -> OsString {
    #[cfg(test)]
    if let Some(p) = PBS_BIN_OVERRIDE.get() {
        return p.clone();
    }
    env::var_os("PVTOOLS_PBS_BIN").unwrap_or_else(|| OsString::from("proxmox-backup-client"))
}

pub fn ensure(repo: &str, ns: &str, envs: &[(String, String)], dry_run: bool) -> Result<()> {
    if exists(repo, ns, envs) {
        tracing::debug!("namespace '{}' exists on {}", ns, repo);
        return Ok(());
    }

    if dry_run {
        let mut prefix = String::new();
        if envs.iter().any(|(k, _)| k == "PBS_PASSWORD") {
            prefix.push_str("PBS_PASSWORD=<redacted> ");
        }
        let cmdline = format!(
            "{}{} {}",
            prefix,
            pbs_bin().to_string_lossy(),
            ["namespace", "create", ns, "--repository", repo]
                .iter()
                .map(|a| sh_quote(a))
                .collect::<Vec<_>>()
                .join(" ")
        );
        tracing::info!(
            "DRY-RUN: namespace '{}' not found on {}; would run:",
            ns,
            repo
        );
        tracing::info!("DRY-RUN: {}", cmdline);
        return Ok(());
    }

    tracing::info!("namespace '{}' not found on {}, creating…", ns, repo);
    create(repo, ns, envs)?;
    if !exists(repo, ns, envs) {
        bail!(
            "namespace '{}' still not visible after create on {}",
            ns,
            repo
        );
    }
    Ok(())
}

pub fn exists(repo: &str, ns: &str, envs: &[(String, String)]) -> bool {
    let mut cmd = Command::new(pbs_bin());
    cmd.args(["namespace", "list", "--repository", repo])
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    for (k, v) in envs {
        cmd.env(k, v);
    }

    match cmd.output() {
        Ok(out) if out.status.success() => {
            let s = String::from_utf8_lossy(&out.stdout);
            s.lines()
                .any(|line| line.split_whitespace().any(|tok| tok == ns))
        }
        _ => false,
    }
}

fn create(repo: &str, ns: &str, envs: &[(String, String)]) -> Result<()> {
    let mut cmd = Command::new(pbs_bin());
    cmd.args(["namespace", "create", ns, "--repository", repo])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let status = cmd
        .status()
        .context("run proxmox-backup-client namespace create")?;
    if status.success() {
        Ok(())
    } else {
        bail!("namespace create failed with {status}")
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[cfg(unix)]
    fn install_fake_pbs(tmp: &TempDir) -> (String, String) {
        let bin_path = tmp.path().join("pbs.sh");
        let state_path = tmp.path().join("state.txt");

        let script = r#"#!/bin/sh
STATE="${PVTOOLS_NS_STATE}"
if [ "$1" = "namespace" ] && [ "$2" = "list" ]; then
  if [ -f "$STATE" ]; then
    cat "$STATE"
  else
    echo "Name"
  fi
  exit 0
fi

if [ "$1" = "namespace" ] && [ "$2" = "create" ]; then
  ns="$3"
  echo "$ns" > "$STATE"
  exit 0
fi

echo "unexpected args: $@" >&2
exit 1
"#;
        fs::write(&bin_path, script).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perm = fs::metadata(&bin_path).unwrap().permissions();
            perm.set_mode(0o755);
            fs::set_permissions(&bin_path, perm).unwrap();
        }

        (
            bin_path.display().to_string(),
            state_path.display().to_string(),
        )
    }

    #[test]
    #[cfg(unix)]
    fn exists_false_then_create_then_exists_true() {
        let tmp = TempDir::new().unwrap();
        let (bin, state) = install_fake_pbs(&tmp);

        PBS_BIN_OVERRIDE.set(bin.clone().into()).ok();

        let repo = "dummy-repo";
        let envs: Vec<(String, String)> = vec![
            ("PBS_PASSWORD".into(), "sekret".into()),
            ("PVTOOLS_NS_STATE".into(), state.clone()),
        ];

        assert!(!exists(repo, "pv", &envs));

        ensure(repo, "pv", &envs, false).expect("ensure create ok");

        assert!(exists(repo, "pv", &envs));
    }

    #[test]
    #[cfg(unix)]
    fn ensure_dry_run_does_not_create() {
        let tmp = TempDir::new().unwrap();
        let (bin, state) = install_fake_pbs(&tmp);

        PBS_BIN_OVERRIDE.set(bin.clone().into()).ok();

        let repo = "dummy-repo";
        let envs: Vec<(String, String)> = vec![
            ("PVTOOLS_NS_STATE".into(), state.clone()),
            ("PBS_PASSWORD".into(), "sekret".into()),
        ];

        // dry-run: печатаем команду, но не создаём
        ensure(repo, "pv", &envs, true).expect("dry-run ok");
        assert!(!exists(repo, "pv", &envs));
        assert!(!std::path::Path::new(&state).exists());
    }

    #[test]
    #[cfg(unix)]
    fn exists_true_when_list_outputs_name() {
        let tmp = TempDir::new().unwrap();
        let (bin, state) = install_fake_pbs(&tmp);

        PBS_BIN_OVERRIDE.set(bin.clone().into()).ok();

        fs::write(&state, "pv\n").unwrap();

        let repo = "dummy-repo";
        let envs: Vec<(String, String)> = vec![("PVTOOLS_NS_STATE".into(), state.clone())];

        assert!(exists(repo, "pv", &envs));
        assert!(!exists(repo, "other", &envs));
    }
}
