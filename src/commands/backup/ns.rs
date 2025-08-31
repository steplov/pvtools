use crate::utils::process::{CmdSpec, EnvValue, Pipeline, ProcessRunner, Runner, StdioSpec};
use anyhow::{Context, Result, bail};

pub fn ensure(
    repo: &str,
    ns: &str,
    envs: &[(String, EnvValue)],
    dry_run: bool,
    runner: &ProcessRunner,
) -> Result<()> {
    if exists(repo, ns, envs, runner) {
        tracing::debug!("namespace '{}' exists on {}", ns, repo);
        return Ok(());
    }

    let cmd = CmdSpec::new("proxmox-backup-client")
        .args(["namespace", "create", ns, "--repository", repo])
        .envs(envs.to_vec())
        .stdout(StdioSpec::Inherit)
        .stderr(StdioSpec::Inherit);

    if dry_run {
        tracing::info!(
            "DRY-RUN: namespace '{}' not found on {}; would run:\n{}",
            ns,
            repo,
            cmd.render()
        );
        return Ok(());
    }

    tracing::info!("namespace '{}' not found on {}, creating…", ns, repo);
    runner
        .run(&Pipeline::new().cmd(cmd))
        .context("run proxmox-backup-client namespace create")?;

    if !exists(repo, ns, envs, runner) {
        bail!(
            "namespace '{}' still not visible after create on {}",
            ns,
            repo
        );
    }
    Ok(())
}

pub fn exists(repo: &str, ns: &str, envs: &[(String, EnvValue)], runner: &ProcessRunner) -> bool {
    let cmd = CmdSpec::new("proxmox-backup-client")
        .args(["namespace", "list", "--repository", repo])
        .envs(envs.to_vec())
        .stdout(StdioSpec::Pipe)
        .stderr(StdioSpec::Null);

    match runner.run_capture(&Pipeline::new().cmd(cmd)) {
        Ok(out) => out
            .lines()
            .any(|line| line.split_whitespace().any(|tok| tok == ns)),
        Err(_) => false,
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

        let runner = ProcessRunner::new().with_override("proxmox-backup-client", &bin);

        let repo = "dummy-repo";
        let envs: Vec<(String, EnvValue)> = vec![
            ("PBS_PASSWORD".into(), EnvValue::Secret("sekret".into())),
            ("PVTOOLS_NS_STATE".into(), EnvValue::Plain(state.clone())),
        ];

        assert!(!exists(repo, "pv", &envs, &runner));

        ensure(repo, "pv", &envs, false, &runner).expect("ensure create ok");

        assert!(exists(repo, "pv", &envs, &runner));
    }

    #[test]
    #[cfg(unix)]
    fn ensure_dry_run_does_not_create() {
        let tmp = TempDir::new().unwrap();
        let (bin, state) = install_fake_pbs(&tmp);

        let runner = ProcessRunner::new().with_override("proxmox-backup-client", &bin);

        let repo = "dummy-repo";
        let envs: Vec<(String, EnvValue)> = vec![
            ("PVTOOLS_NS_STATE".into(), EnvValue::Plain(state.clone())),
            ("PBS_PASSWORD".into(), EnvValue::Secret("sekret".into())),
        ];

        ensure(repo, "pv", &envs, true, &runner).expect("dry-run ok");
        assert!(!exists(repo, "pv", &envs, &runner));
        assert!(!std::path::Path::new(&state).exists());
    }

    #[test]
    #[cfg(unix)]
    fn exists_true_when_list_outputs_name() {
        let tmp = TempDir::new().unwrap();
        let (bin, state) = install_fake_pbs(&tmp);

        let runner = ProcessRunner::new().with_override("proxmox-backup-client", &bin);

        fs::write(&state, "pv\n").unwrap();

        let repo = "dummy-repo";
        let envs: Vec<(String, EnvValue)> =
            vec![("PVTOOLS_NS_STATE".into(), EnvValue::Plain(state.clone()))];

        assert!(exists(repo, "pv", &envs, &runner));
        assert!(!exists(repo, "other", &envs, &runner));
    }
}
