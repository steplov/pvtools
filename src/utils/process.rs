use std::{
    collections::HashMap,
    path::PathBuf,
    process::{Child, Command, Stdio},
};

use anyhow::{Context, Result, anyhow, bail};

use crate::utils::exec_policy;

#[derive(Clone, Debug)]
pub enum EnvValue {
    Plain(String),
    Secret(String),
}

#[derive(Clone, Debug)]
pub enum StdioSpec {
    Inherit,
    Null,
    Pipe,
}

impl StdioSpec {
    #[inline]
    fn to_stdio(&self) -> Stdio {
        match self {
            StdioSpec::Inherit => Stdio::inherit(),
            StdioSpec::Null => Stdio::null(),
            StdioSpec::Pipe => Stdio::piped(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct CmdSpec {
    program: String,
    args: Vec<String>,
    envs: Vec<(String, EnvValue)>,
    stdin: StdioSpec,
    stdout: StdioSpec,
    stderr: StdioSpec,
    cwd: Option<PathBuf>,
}

impl CmdSpec {
    #[must_use]
    pub fn new<S: Into<String>>(program: S) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            envs: Vec::new(),
            stdin: StdioSpec::Inherit,
            stdout: StdioSpec::Inherit,
            stderr: StdioSpec::Inherit,
            cwd: None,
        }
    }

    #[must_use]
    pub fn arg(mut self, a: impl Into<String>) -> Self {
        self.args.push(a.into());
        self
    }

    #[must_use]
    pub fn args<I, S>(mut self, it: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.args.extend(it.into_iter().map(Into::into));
        self
    }

    #[must_use]
    pub fn env(mut self, k: impl Into<String>, v: EnvValue) -> Self {
        self.envs.push((k.into(), v));
        self
    }

    #[must_use]
    pub fn envs<I>(mut self, vars: I) -> Self
    where
        I: IntoIterator<Item = (String, EnvValue)>,
    {
        self.envs.extend(vars);
        self
    }

    #[must_use]
    pub fn stdin(mut self, s: StdioSpec) -> Self {
        self.stdin = s;
        self
    }

    #[must_use]
    pub fn stdout(mut self, s: StdioSpec) -> Self {
        self.stdout = s;
        self
    }

    #[must_use]
    pub fn stderr(mut self, s: StdioSpec) -> Self {
        self.stderr = s;
        self
    }
    #[must_use]
    pub fn cwd<P: Into<PathBuf>>(mut self, dir: P) -> Self {
        self.cwd = Some(dir.into());
        self
    }

    pub fn render(&self) -> String {
        let prog = sh_quote(&self.program);
        let args: Vec<String> = self.args.iter().map(|a| sh_quote(a)).collect();
        let mut env_prefix = String::new();
        for (k, v) in &self.envs {
            match v {
                EnvValue::Plain(val) => env_prefix.push_str(&format!("{k}={} ", sh_quote(val))),
                EnvValue::Secret(_) => env_prefix.push_str(&format!("{k}=<redacted> ")),
            }
        }
        format!("{}{} {}", env_prefix, prog, args.join(" "))
    }
    fn to_command(&self, bin: &str) -> Command {
        let mut cmd = Command::new(bin);
        cmd.args(&self.args);
        for (k, v) in &self.envs {
            match v {
                EnvValue::Plain(val) => cmd.env(k, val),
                EnvValue::Secret(val) => cmd.env(k, val),
            };
        }
        if let Some(ref d) = self.cwd {
            cmd.current_dir(d);
        }
        cmd
    }
}

#[derive(Clone, Debug, Default)]
pub struct Pipeline {
    pub cmds: Vec<CmdSpec>,
}

impl Pipeline {
    pub fn new() -> Self {
        Self { cmds: Vec::new() }
    }

    #[must_use]
    pub fn cmd(mut self, c: CmdSpec) -> Self {
        self.cmds.push(c);
        self
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.cmds.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.cmds.is_empty()
    }

    pub fn render(&self) -> String {
        self.cmds
            .iter()
            .map(|c| c.render())
            .collect::<Vec<_>>()
            .join(" | ")
    }
}

pub trait Runner: Send + Sync {
    fn run(&self, pipeline: &Pipeline) -> Result<()>;
    fn run_capture(&self, pipeline: &Pipeline) -> Result<String>;
}

#[derive(Default, Clone)]
pub struct ProcessRunner {
    bin_overrides: HashMap<String, String>,
}

impl ProcessRunner {
    pub fn new() -> Self {
        Self {
            bin_overrides: HashMap::new(),
        }
    }

    fn resolve_bin<'a>(&'a self, bin: &'a str) -> &'a str {
        self.bin_overrides
            .get(bin)
            .map(|s| s.as_str())
            .unwrap_or(bin)
    }
}

impl Runner for ProcessRunner {
    fn run(&self, pipeline: &Pipeline) -> Result<()> {
        if exec_policy::is_dry_run() {
            tracing::info!("[DRY-RUN] {}", pipeline.render());
            return Ok(());
        }
        tracing::debug!("exec: {}", pipeline.render());

        let n = pipeline.len();
        if n == 0 {
            bail!("empty pipeline");
        }

        let mut children: Vec<Child> = Vec::with_capacity(n);
        let mut prev_stdout: Option<Stdio> = None;

        for (i, spec) in pipeline.cmds.iter().enumerate() {
            let bin = self.resolve_bin(&spec.program);
            let mut cmd = spec.to_command(bin);

            if i == 0 {
                cmd.stdin(spec.stdin.to_stdio());
            } else {
                let stdin = prev_stdout
                    .take()
                    .ok_or_else(|| anyhow!("internal pipe error at stage {}", i))?;
                cmd.stdin(stdin);
            }

            if i == n - 1 {
                cmd.stdout(spec.stdout.to_stdio());
            } else {
                cmd.stdout(Stdio::piped());
            }

            cmd.stderr(spec.stderr.to_stdio());

            let mut child = cmd
                .spawn()
                .with_context(|| format!("spawn {}", spec.render()))?;

            prev_stdout = if i == n - 1 {
                None
            } else {
                Some(Stdio::from(child.stdout.take().ok_or_else(|| {
                    anyhow!("stdout piping not available at stage {}", i)
                })?))
            };

            children.push(child);
        }

        for (i, mut child) in children.into_iter().enumerate() {
            let status = child
                .wait()
                .with_context(|| format!("wait for stage {}: {}", i, pipeline.render()))?;
            if !status.success() {
                bail!("command failed: {} with {status}", pipeline.render());
            }
        }
        Ok(())
    }

    fn run_capture(&self, pipeline: &Pipeline) -> Result<String> {
        tracing::debug!("exec(capture): {}", pipeline.render());

        if pipeline.len() != 1 {
            bail!(
                "capture only works with single command, got {}",
                pipeline.len()
            );
        }
        let spec = &pipeline.cmds[0];
        let bin = self.resolve_bin(&spec.program);
        let mut cmd = spec.to_command(bin);

        cmd.stdout(Stdio::piped());
        cmd.stderr(spec.stderr.to_stdio());
        cmd.stdin(spec.stdin.to_stdio());

        let out = cmd
            .output()
            .with_context(|| format!("run {}", spec.render()))?;
        if out.status.success() {
            Ok(String::from_utf8_lossy(&out.stdout).to_string())
        } else {
            bail!("command failed: {} (status {})", spec.render(), out.status);
        }
    }
}

fn sh_quote(s: &str) -> String {
    if s.is_empty() {
        return "''".into();
    }
    if !s
        .bytes()
        .any(|b| b == b' ' || b == b'\'' || b == b'"' || b == b'\\')
    {
        return s.to_string();
    }
    let mut out = String::from("'");
    for c in s.chars() {
        if c == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(c);
        }
    }
    out.push('\'');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sh_quote_empty() {
        assert_eq!(sh_quote(""), "''");
    }

    #[test]
    fn sh_quote_simple() {
        assert_eq!(sh_quote("hello"), "hello");
    }

    #[test]
    fn sh_quote_with_space() {
        assert_eq!(sh_quote("hello world"), "'hello world'");
    }

    #[test]
    fn sh_quote_with_single_quote() {
        assert_eq!(sh_quote("don't"), "'don'\\''t'");
    }

    #[test]
    fn cmd_spec_render() {
        let cmd = CmdSpec::new("ls").arg("-l").arg("file name");
        assert_eq!(cmd.render(), "ls -l 'file name'");
    }

    #[test]
    fn cmd_spec_with_env() {
        let cmd = CmdSpec::new("cmd")
            .env("VAR", EnvValue::Plain("value".into()))
            .env("SECRET", EnvValue::Secret("hidden".into()));
        assert_eq!(cmd.render(), "VAR=value SECRET=<redacted> cmd ");
    }

    #[test]
    fn pipeline_render() {
        let pipeline = Pipeline::new()
            .cmd(CmdSpec::new("cat").arg("file"))
            .cmd(CmdSpec::new("grep").arg("pattern"));
        assert_eq!(pipeline.render(), "cat file | grep pattern");
    }

    #[test]
    fn pipeline_empty() {
        let pipeline = Pipeline::new();
        assert!(pipeline.is_empty());
        assert_eq!(pipeline.len(), 0);
    }
}
