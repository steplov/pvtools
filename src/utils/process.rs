use anyhow::{Context, Result, bail};
use std::collections::HashMap;
use std::process::{Child, Command, Stdio};

/// Value for environment variables
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum EnvValue {
    Plain(String),
    Secret(String),
}

/// How to configure stdio
#[derive(Clone, Debug)]
pub enum StdioSpec {
    Inherit,
    Null,
    Pipe,
}

/// Command specification
#[derive(Clone, Debug)]
pub struct CmdSpec {
    pub program: String,
    pub args: Vec<String>,
    pub envs: Vec<(String, EnvValue)>,
    pub stdin: StdioSpec,
    pub stdout: StdioSpec,
    pub stderr: StdioSpec,
}

#[allow(dead_code)]
impl CmdSpec {
    pub fn new<S: Into<String>>(program: S) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            envs: Vec::new(),
            stdin: StdioSpec::Inherit,
            stdout: StdioSpec::Inherit,
            stderr: StdioSpec::Inherit,
        }
    }

    pub fn arg(mut self, a: impl Into<String>) -> Self {
        self.args.push(a.into());
        self
    }

    pub fn args<I, S>(mut self, it: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.args.extend(it.into_iter().map(Into::into));
        self
    }

    pub fn env(mut self, k: impl Into<String>, v: EnvValue) -> Self {
        self.envs.push((k.into(), v));
        self
    }

    pub fn envs<I>(mut self, vars: I) -> Self
    where
        I: IntoIterator<Item = (String, EnvValue)>,
    {
        self.envs.extend(vars);
        self
    }

    pub fn stdin(mut self, s: StdioSpec) -> Self {
        self.stdin = s;
        self
    }
    pub fn stdout(mut self, s: StdioSpec) -> Self {
        self.stdout = s;
        self
    }
    pub fn stderr(mut self, s: StdioSpec) -> Self {
        self.stderr = s;
        self
    }

    /// Render this command with redacted secrets
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
}

/// A sequence of commands, possibly piped
#[derive(Clone, Debug, Default)]
pub struct Pipeline {
    pub cmds: Vec<CmdSpec>,
}

impl Pipeline {
    pub fn new() -> Self {
        Self { cmds: Vec::new() }
    }

    pub fn cmd(mut self, c: CmdSpec) -> Self {
        self.cmds.push(c);
        self
    }

    /// Base rendering for dry-run/logging
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

#[allow(dead_code)]
impl ProcessRunner {
    pub fn new() -> Self {
        Self {
            bin_overrides: HashMap::new(),
        }
    }

    pub fn with_override(mut self, bin: &str, path: &str) -> Self {
        self.bin_overrides.insert(bin.to_string(), path.to_string());
        self
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
        let mut children: Vec<Child> = Vec::new();
        let mut prev_stdout = None;

        for (i, spec) in pipeline.cmds.iter().enumerate() {
            let bin = self.resolve_bin(&spec.program);
            let mut cmd = Command::new(bin);
            cmd.args(&spec.args);

            // stdin
            if i == 0 {
                cmd.stdin(match spec.stdin {
                    StdioSpec::Inherit => Stdio::inherit(),
                    StdioSpec::Null => Stdio::null(),
                    StdioSpec::Pipe => Stdio::piped(),
                });
            } else {
                cmd.stdin(prev_stdout.take().unwrap());
            }

            // stdout
            if i == pipeline.cmds.len() - 1 {
                cmd.stdout(match spec.stdout {
                    StdioSpec::Inherit => Stdio::inherit(),
                    StdioSpec::Null => Stdio::null(),
                    StdioSpec::Pipe => Stdio::piped(),
                });
            } else {
                cmd.stdout(Stdio::piped());
            }

            // stderr
            cmd.stderr(match spec.stderr {
                StdioSpec::Inherit => Stdio::inherit(),
                StdioSpec::Null => Stdio::null(),
                StdioSpec::Pipe => Stdio::piped(),
            });

            // env
            for (k, v) in &spec.envs {
                match v {
                    EnvValue::Plain(val) => cmd.env(k, val),
                    EnvValue::Secret(val) => cmd.env(k, val),
                };
            }

            let mut child = cmd
                .spawn()
                .with_context(|| format!("spawn {}", spec.render()))?;

            prev_stdout = child.stdout.take().map(Stdio::from);
            children.push(child);
        }

        for mut child in children {
            let status = child
                .wait()
                .with_context(|| format!("wait for {}", pipeline.render()))?;
            if !status.success() {
                bail!("command failed: {} with {status}", pipeline.render());
            }
        }
        Ok(())
    }

    fn run_capture(&self, pipeline: &Pipeline) -> Result<String> {
        if pipeline.cmds.len() != 1 {
            bail!(
                "capture only works with single command, got {}",
                pipeline.cmds.len()
            );
        }
        let spec = &pipeline.cmds[0];
        let bin = self.resolve_bin(&spec.program);
        let mut cmd = Command::new(bin);
        cmd.args(&spec.args);
        for (k, v) in &spec.envs {
            match v {
                EnvValue::Plain(val) => cmd.env(k, val),
                EnvValue::Secret(val) => cmd.env(k, val),
            };
        }
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
