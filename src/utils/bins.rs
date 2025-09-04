use std::{
    env,
    path::{Path, PathBuf},
};

use anyhow::{Result, anyhow};

pub fn ensure_bins<I, S>(bins: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut missing = Vec::new();
    for b in bins {
        let b = b.as_ref();
        if which(b).is_none() {
            missing.push(b.to_string());
        }
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "missing required binaries in PATH: {}",
            missing.join(", ")
        ))
    }
}

pub fn which(bin: &str) -> Option<PathBuf> {
    let p = Path::new(bin);
    if p.is_absolute() && is_executable(p) {
        return Some(p.to_path_buf());
    }
    let path = env::var_os("PATH")?;
    for dir in env::split_paths(&path) {
        let cand = dir.join(bin);
        if is_executable(&cand) {
            return Some(cand);
        }
    }
    None
}

fn is_executable(p: &Path) -> bool {
    let Ok(meta) = std::fs::metadata(p) else {
        return false;
    };
    if !meta.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        meta.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}
