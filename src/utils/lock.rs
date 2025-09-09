#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use fs2::FileExt;

pub struct LockGuard {
    file: File,
    path: PathBuf,
}

impl std::fmt::Debug for LockGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LockGuard")
            .field("path", &self.path)
            .finish()
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

impl LockGuard {
    pub fn try_acquire(name: &str) -> Result<Self> {
        let path = lock_path_for(name);
        ensure_parent_dir(&path)?;
        let file = open_lockfile(&path)?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(Self { file, path }),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                bail!("another run holds lock: {}", path.display())
            }
            Err(e) => Err(e).with_context(|| format!("flock {}", path.display())),
        }
    }
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(dir) = path.parent()
        && !dir.exists()
    {
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            let mut b = fs::DirBuilder::new();
            b.recursive(true)
                .mode(0o755)
                .create(dir)
                .with_context(|| format!("create lock dir {}", dir.display()))?;
        }
        #[cfg(not(unix))]
        {
            fs::create_dir_all(dir)
                .with_context(|| format!("create lock dir {}", dir.display()))?;
        }
    }
    Ok(())
}

fn open_lockfile(path: &Path) -> Result<File> {
    let mut opts = OpenOptions::new();
    opts.read(true).write(true).create(true);
    #[cfg(unix)]
    {
        opts.mode(0o644);
    }
    opts.open(path)
        .with_context(|| format!("open lockfile {}", path.display()))
}

fn lock_path_for(name: &str) -> PathBuf {
    let safe = sanitize_name(name);
    let candidate = PathBuf::from("/var/lock").join(format!("{safe}.lock"));
    if can_use_dir(candidate.parent().unwrap()) {
        candidate
    } else {
        std::env::temp_dir().join(format!("{safe}.lock"))
    }
}

fn can_use_dir(dir: &Path) -> bool {
    if !dir.is_dir() {
        return false;
    }
    let test = dir.join(".pvtool_lock_test");
    match OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&test)
    {
        Ok(_) => {
            let _ = fs::remove_file(test);
            true
        }
        Err(_) => false,
    }
}

fn sanitize_name(s: &str) -> String {
    let filtered: String = s
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
        .collect();

    if filtered.is_empty() {
        "lock_".to_string()
    } else {
        format!("lock_{filtered}")
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn acquire_and_release() {
        let name = format!("lock-basic-{}", rand_suffix());
        let g1 = LockGuard::try_acquire(&name).expect("first acquire ok");
        drop(g1);

        let _g2 = LockGuard::try_acquire(&name).expect("re-acquire ok after drop");
    }

    #[test]
    fn conflict_same_name() {
        let name = format!("lock-conflict-{}", rand_suffix());
        let _g1 = LockGuard::try_acquire(&name).expect("first acquire ok");
        let err = LockGuard::try_acquire(&name).unwrap_err().to_string();
        assert!(err.contains("another run holds lock"), "err was: {err}");
    }

    #[test]
    fn ensure_parent_dir_creates_missing_dirs() {
        let temp = TempDir::new().unwrap();
        let sub = temp.path().join("nested/dir/for/locks");
        let file = sub.join("x.lock");
        ensure_parent_dir(&file).expect("ensure parent dir");
        assert!(sub.exists() && sub.is_dir());
        let _f = open_lockfile(&file).expect("open lockfile");
    }

    #[test]
    fn sanitize_always_prefix() {
        assert_eq!(sanitize_name(".foo"), "lock_foo");
        assert_eq!(sanitize_name("-bar"), "lock_-bar");
        assert_eq!(sanitize_name("ok_Name-123"), "lock_ok_Name-123");
        assert_eq!(sanitize_name(".."), "lock_");
    }

    #[test]
    fn lock_path_for_points_to_var_or_tmp() {
        let p = lock_path_for(&format!("lp-{}", rand_suffix()));
        let parent = p.parent().unwrap();
        let tmp = std::env::temp_dir();
        assert!(
            parent.starts_with("/var/lock") || parent.starts_with(&tmp),
            "parent={parent:?} tmp={tmp:?}"
        );
        assert!(p.file_name().unwrap().to_string_lossy().ends_with(".lock"));
    }

    fn rand_suffix() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{ns}")
    }
}
